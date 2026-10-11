import Foundation
import GRDB

enum RecordingStatus: String, Codable {
    case pending
    case converting
    case transcribing
    case completed
    case failed
}

enum QueueArtifactState: String, Codable {
    case staging
}

enum RecordingDeletionState: String, Codable {
    case deleting
}

struct Recording: Identifiable, Codable, FetchableRecord, PersistableRecord, Equatable {
    let id: UUID
    let timestamp: Date
    let fileName: String
    var transcription: String
    let duration: TimeInterval
    var status: RecordingStatus
    var progress: Float
    var sourceFileURL: String?
    var preparedPCMFileURL: String? = nil
    var queueArtifactState: QueueArtifactState? = nil
    var deletionState: RecordingDeletionState? = nil
    
    var isRegeneration: Bool = false
    
    enum CodingKeys: String, CodingKey {
        case id, timestamp, fileName, transcription, duration, status, progress, sourceFileURL,
             preparedPCMFileURL, queueArtifactState, deletionState
    }

    static func == (lhs: Recording, rhs: Recording) -> Bool {
        return lhs.id == rhs.id &&
               lhs.status == rhs.status &&
               lhs.deletionState == rhs.deletionState &&
               lhs.progress == rhs.progress &&
               lhs.transcription == rhs.transcription &&
               lhs.isRegeneration == rhs.isRegeneration
    }

    static var recordingsDirectory: URL {
        let applicationSupport = FileManager.default.urls(
            for: .applicationSupportDirectory, in: .userDomainMask
        ).first!
        let appDirectory = applicationSupport.appendingPathComponent(Bundle.main.bundleIdentifier!)
        return appDirectory.appendingPathComponent("recordings")
    }

    static func fileName(for id: UUID) -> String {
        "\(id.uuidString).wav"
    }

    var url: URL {
        Self.recordingsDirectory.appendingPathComponent(fileName)
    }
    
    var isPending: Bool {
        deletionState == nil && (status == .pending || status == .converting || status == .transcribing)
    }
    
    var sourceFileName: String? {
        guard let sourceFileURL = sourceFileURL else { return nil }
        return URL(fileURLWithPath: sourceFileURL).lastPathComponent
    }

    static let databaseTableName = "recordings"

    enum Columns {
        static let id = Column(CodingKeys.id)
        static let timestamp = Column(CodingKeys.timestamp)
        static let fileName = Column(CodingKeys.fileName)
        static let transcription = Column(CodingKeys.transcription)
        static let duration = Column(CodingKeys.duration)
        static let status = Column(CodingKeys.status)
        static let progress = Column(CodingKeys.progress)
        static let sourceFileURL = Column(CodingKeys.sourceFileURL)
        static let preparedPCMFileURL = Column(CodingKeys.preparedPCMFileURL)
        static let queueArtifactState = Column(CodingKeys.queueArtifactState)
        static let deletionState = Column(CodingKeys.deletionState)
    }
}

enum QueueArtifactCheckpoint: CaseIterable {
    case beforeSourceMove
    case afterSourceMove
    case duringPreparedPCMWrite
    case afterPreparedPCMWrite
    case afterActivation
}

@MainActor
final class QueueArtifactLifecycleCoordinator {
    private var isRunning = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var activeDeletionIDs: Set<UUID> = []

    func perform<T>(_ operation: () async throws -> T) async rethrows -> T {
        await acquire()
        defer { release() }
        return try await operation()
    }

    func reserveDeletion(_ id: UUID) {
        activeDeletionIDs.insert(id)
    }

    func releaseDeletion(_ id: UUID) {
        activeDeletionIDs.remove(id)
    }

    func deletionIsActive(_ id: UUID) -> Bool {
        activeDeletionIDs.contains(id)
    }

    private func acquire() async {
        if !isRunning {
            isRunning = true
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    private func release() {
        guard !waiters.isEmpty else {
            isRunning = false
            return
        }
        waiters.removeFirst().resume()
    }
}

@MainActor
class RecordingStore: ObservableObject {
    static let shared = RecordingStore()

    @Published private(set) var recordings: [Recording] = []
    private let dbQueue: DatabaseQueue?
    private let initializationError: Error?
    private let reconcilesOrphanedArtifacts: Bool
    private let queueArtifactCheckpoint: ((QueueArtifactCheckpoint, Recording) throws -> Void)?
    private let queueArtifactLifecycle = QueueArtifactLifecycleCoordinator()

    private convenience init() {
        let directory = Recording.recordingsDirectory.deletingLastPathComponent()
        self.init(
            databaseURL: directory.appendingPathComponent("recordings.sqlite"),
            reconcilesOrphanedArtifacts: true
        )
    }

    init(
        databaseURL: URL,
        reconcilesOrphanedArtifacts: Bool = false,
        queueArtifactCheckpoint: ((QueueArtifactCheckpoint, Recording) throws -> Void)? = nil
    ) {
        self.reconcilesOrphanedArtifacts = reconcilesOrphanedArtifacts
        self.queueArtifactCheckpoint = queueArtifactCheckpoint
        do {
            try FileManager.default.createDirectory(at: databaseURL.deletingLastPathComponent(), withIntermediateDirectories: true)
            let database = try DatabaseQueue(path: databaseURL.path)
            try Self.setupDatabase(database)
            dbQueue = database
            initializationError = nil
        } catch {
            dbQueue = nil
            initializationError = error
            AppErrorCenter.shared.report("Recordings database is unavailable", error: error)
        }
    }

    init(
        databaseQueue: DatabaseQueue,
        queueArtifactCheckpoint: ((QueueArtifactCheckpoint, Recording) throws -> Void)? = nil
    ) throws {
        reconcilesOrphanedArtifacts = false
        self.queueArtifactCheckpoint = queueArtifactCheckpoint
        try Self.setupDatabase(databaseQueue)
        dbQueue = databaseQueue
        initializationError = nil
    }

    private nonisolated func database() throws -> DatabaseQueue {
        if let initializationError { throw initializationError }
        guard let dbQueue else { throw TranscriptionError.processingFailed }
        return dbQueue
    }

    private nonisolated static func setupDatabase(_ dbQueue: DatabaseQueue) throws {
        var migrator = DatabaseMigrator()
        
        migrator.registerMigration("v1") { db in
            try db.create(table: Recording.databaseTableName, ifNotExists: true) { t in
                t.column("id", .text).primaryKey()
                t.column("timestamp", .datetime).notNull().indexed()
                t.column("fileName", .text).notNull()
                t.column("transcription", .text).notNull().indexed().collate(.nocase)
                t.column("duration", .double).notNull()
            }
        }
        
        migrator.registerMigration("v2_add_status") { db in
            let columns = try db.columns(in: Recording.databaseTableName)
            let columnNames = columns.map { $0.name }
            
            if !columnNames.contains("status") {
                try db.alter(table: Recording.databaseTableName) { t in
                    t.add(column: "status", .text).notNull().defaults(to: "completed")
                }
            }
            if !columnNames.contains("progress") {
                try db.alter(table: Recording.databaseTableName) { t in
                    t.add(column: "progress", .double).notNull().defaults(to: 1.0)
                }
            }
            if !columnNames.contains("sourceFileURL") {
                try db.alter(table: Recording.databaseTableName) { t in
                    t.add(column: "sourceFileURL", .text)
                }
            }
        }

        migrator.registerMigration("v3_add_prepared_pcm_file_url") { db in
            let columns = try db.columns(in: Recording.databaseTableName)
            if !columns.contains(where: { $0.name == "preparedPCMFileURL" }) {
                try db.alter(table: Recording.databaseTableName) { table in
                    table.add(column: "preparedPCMFileURL", .text)
                }
            }
        }

        migrator.registerMigration("v4_add_queue_artifact_state") { db in
            let columns = try db.columns(in: Recording.databaseTableName)
            if !columns.contains(where: { $0.name == "queueArtifactState" }) {
                try db.alter(table: Recording.databaseTableName) { table in
                    table.add(column: "queueArtifactState", .text)
                }
            }
        }

        migrator.registerMigration("v5_add_deletion_state") { db in
            let columns = try db.columns(in: Recording.databaseTableName)
            if !columns.contains(where: { $0.name == "deletionState" }) {
                try db.alter(table: Recording.databaseTableName) { table in
                    table.add(column: "deletionState", .text)
                }
            }
        }
        
        try migrator.migrate(dbQueue)
    }
    
    private nonisolated func fetchAllRecordings() async throws -> [Recording] {
        try await database().read { db in
            try Recording
                .order(Recording.Columns.timestamp.desc)
                .fetchAll(db)
        }
    }
    
    nonisolated func fetchRecordings(limit: Int, offset: Int) async throws -> [Recording] {
        try await database().read { db in
            try Recording
                .order(Recording.Columns.timestamp.desc)
                .limit(limit, offset: offset)
                .fetchAll(db)
        }
    }

    func getPendingRecordings() throws -> [Recording] {
        return try database().read { db in
            try Recording
                .filter([RecordingStatus.pending.rawValue, RecordingStatus.converting.rawValue, RecordingStatus.transcribing.rawValue].contains(Recording.Columns.status))
                .filter(Recording.Columns.deletionState == nil)
                .order(Recording.Columns.timestamp.asc)
                .fetchAll(db)
        }
    }

    func getNextPendingRecording() throws -> Recording? {
        return try database().read { db in
            try Recording
                .filter([RecordingStatus.pending.rawValue, RecordingStatus.converting.rawValue, RecordingStatus.transcribing.rawValue].contains(Recording.Columns.status))
                .filter(Recording.Columns.deletionState == nil)
                .order(Recording.Columns.timestamp.asc)
                .limit(1)
                .fetchOne(db)
        }
    }

    static let recordingsDidUpdateNotification = Notification.Name("RecordingStore.recordingsDidUpdate")

    func addQueuedRecording(url: URL, duration: TimeInterval, pcmSamples: [Float]?) async throws {
        let id = UUID()
        let fileName = Recording.fileName(for: id)
        guard let pcmSamples else {
            let recording = Recording(
                id: id,
                timestamp: Date(),
                fileName: fileName,
                transcription: "",
                duration: duration,
                status: .pending,
                progress: 0,
                sourceFileURL: url.path
            )
            try await addRecordingSync(recording)
            return
        }

        try await queueArtifactLifecycle.perform {
            let preparedPCMURL = QueuedPCMFile.url(for: id)
            let durableSourceURL = Recording.recordingsDirectory.appendingPathComponent(fileName)
            var recording = Recording(
                id: id,
                timestamp: Date(),
                fileName: fileName,
                transcription: "",
                duration: duration,
                status: .failed,
                progress: 0,
                sourceFileURL: url.path,
                preparedPCMFileURL: preparedPCMURL.path,
                queueArtifactState: .staging
            )
            try await insertRecording(recording)
            try queueArtifactCheckpoint?(.beforeSourceMove, recording)
            if url.standardizedFileURL != durableSourceURL.standardizedFileURL {
                try await moveQueueSource(from: url, to: durableSourceURL)
            }
            try queueArtifactCheckpoint?(.afterSourceMove, recording)
            try await writePreparedPCM(pcmSamples, to: preparedPCMURL, for: recording)
            try queueArtifactCheckpoint?(.afterPreparedPCMWrite, recording)
            recording.sourceFileURL = durableSourceURL.path
            recording.status = .pending
            recording.queueArtifactState = nil
            try await activateStagedRecording(recording)
            try queueArtifactCheckpoint?(.afterActivation, recording)
        }
    }

    func saveFailedDictation(_ audio: RecordedAudio, error: Error) async throws -> Recording {
        try await queueArtifactLifecycle.perform {
            try await saveFailedDictationTransaction(audio, error: error)
        }
    }

    private func saveFailedDictationTransaction(_ audio: RecordedAudio, error: Error) async throws -> Recording {
        let id = UUID()
        let alreadySaved = audio.url.deletingLastPathComponent().standardizedFileURL == Recording.recordingsDirectory.standardizedFileURL
        let name = alreadySaved ? audio.url.lastPathComponent : Recording.fileName(for: id)
        let destination = Recording.recordingsDirectory.appendingPathComponent(name)
        let preparedPCMURL = QueuedPCMFile.url(for: id)
        var row = Recording(
            id: id,
            timestamp: Date(),
            fileName: name,
            transcription: error.localizedDescription,
            duration: audio.duration,
            status: .failed,
            progress: 0,
            sourceFileURL: audio.url.path,
            preparedPCMFileURL: preparedPCMURL.path,
            queueArtifactState: .staging
        )
        do {
            try await insertRecording(row)
        } catch {
            throw PreservedAudioError(url: audio.url, underlying: error)
        }
        do {
            try queueArtifactCheckpoint?(.beforeSourceMove, row)
            if !alreadySaved {
                try await moveQueueSource(from: audio.url, to: destination)
            }
            try queueArtifactCheckpoint?(.afterSourceMove, row)
            try await writePreparedPCM(audio.samples, to: preparedPCMURL, for: row)
            try queueArtifactCheckpoint?(.afterPreparedPCMWrite, row)
            row.sourceFileURL = destination.path
            row.status = .failed
            row.queueArtifactState = nil
            try await activateStagedRecording(row)
            try queueArtifactCheckpoint?(.afterActivation, row)
        } catch {
            let preservedURL = FileManager.default.fileExists(atPath: destination.path)
                ? destination
                : audio.url
            throw PreservedAudioError(url: preservedURL, underlying: error)
        }
        return row
    }

    func preserveFailedDictation(_ audio: RecordedAudio, error: Error) async {
        do {
            let row = try await saveFailedDictation(audio, error: error)
            AppErrorCenter.shared.report("Recording failed", message: "\(error.localizedDescription)\nAudio saved: \(row.url.path)")
        } catch {
            AppErrorCenter.shared.report("Recording could not be added to history",
                message: "\(error.localizedDescription)\nOriginal audio location: \(audio.url.path)")
        }
    }

    func addRecording(_ recording: Recording) {
        Task {
            do {
                try await insertRecording(recording)
                await MainActor.run {
                    NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
                }
            } catch {
                AppErrorCenter.shared.report("Recording could not be saved", error: error)
            }
        }
    }
    
    func addRecordingSync(_ recording: Recording) async throws {
        try await insertRecording(recording)
        await MainActor.run {
            NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
        }
    }
    
    private nonisolated func insertRecording(_ recording: Recording) async throws {
        try await database().write { db in
            try recording.insert(db)
        }
    }

    private func moveQueueSource(from source: URL, to destination: URL) async throws {
        try await Task.detached(priority: .userInitiated) {
            try FileManager.default.createDirectory(
                at: destination.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try FileManager.default.moveItem(at: source, to: destination)
        }.value
    }

    private func writePreparedPCM(
        _ samples: [Float],
        to destination: URL,
        for recording: Recording
    ) async throws {
        let checkpoint = queueArtifactCheckpoint
        try await Task.detached(priority: .userInitiated) {
            try QueuedPCMFile.write(samples, to: destination) { _ in
                try checkpoint?(.duringPreparedPCMWrite, recording)
            }
        }.value
    }

    private func activateStagedRecording(_ recording: Recording) async throws {
        let updated = try await database().write { db in
            try Recording
                .filter(Recording.Columns.id == recording.id)
                .filter(Recording.Columns.queueArtifactState == QueueArtifactState.staging.rawValue)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [
                    Recording.Columns.transcription.set(to: recording.transcription),
                    Recording.Columns.status.set(to: recording.status.rawValue),
                    Recording.Columns.progress.set(to: recording.progress),
                    Recording.Columns.sourceFileURL.set(to: recording.sourceFileURL),
                    Recording.Columns.preparedPCMFileURL.set(to: recording.preparedPCMFileURL),
                    Recording.Columns.queueArtifactState.set(to: nil as String?)
                ])
        }
        guard updated == 1 else { throw TranscriptionError.processingFailed }
        NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
    }
    
    func updateRecording(_ recording: Recording) {
        Task {
            do {
                try await updateRecordingInDB(recording)
                await MainActor.run {
                    NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
                }
            } catch {
                AppErrorCenter.shared.report("Recording could not be updated", error: error)
            }
        }
    }
    
    func updateRecordingSync(_ recording: Recording) async throws {
        try await updateRecordingInDB(recording)
        await MainActor.run {
            NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
        }
    }
    
    func updateRecordingProgressOnly(_ id: UUID, transcription: String, progress: Float, status: RecordingStatus) {
        Task {
            do { try await updateRecordingProgressOnlySync(id, transcription: transcription, progress: progress, status: status) }
            catch { AppErrorCenter.shared.report("Recording could not be updated", error: error) }
        }
    }
    
    static let recordingProgressDidUpdateNotification = Notification.Name("RecordingStore.recordingProgressDidUpdate")
    
    /// Updates the in-memory copy and notifies observers without touching the database.
    private func applyLocalProgressUpdate(_ id: UUID, transcription: String? = nil, progress: Float, status: RecordingStatus, isRegeneration: Bool? = nil) {
        if let index = recordings.firstIndex(where: { $0.id == id }) {
            var updated = recordings[index]
            if let transcription = transcription {
                updated.transcription = transcription
            }
            updated.progress = progress
            updated.status = status
            if let isRegeneration = isRegeneration {
                updated.isRegeneration = isRegeneration
            }
            recordings[index] = updated
        }
        
        var userInfo: [String: Any] = [
            "id": id,
            "progress": progress,
            "status": status
        ]
        if let transcription = transcription {
            userInfo["transcription"] = transcription
        }
        if let isRegeneration = isRegeneration {
            userInfo["isRegeneration"] = isRegeneration
        }
        
        NotificationCenter.default.post(name: Self.recordingProgressDidUpdateNotification, object: nil, userInfo: userInfo)
    }
    
    /// Progress ticks are ephemeral UI state: persisting each one caused up to
    /// ~100 SQLite transactions per transcription. Status transitions are still
    /// persisted by the explicit update methods.
    func updateRecordingProgressTransient(_ id: UUID, progress: Float, status: RecordingStatus) {
        if recordings.first(where: { $0.id == id })?.deletionState != nil { return }
        applyLocalProgressUpdate(id, progress: progress, status: status)
    }
    
    func updateRecordingProgressOnlySync(_ id: UUID, transcription: String, progress: Float, status: RecordingStatus, isRegeneration: Bool? = nil) async throws {
        let updated = try await database().write { db -> Int in
            try Recording
                .filter(Recording.Columns.id == id)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [
                    Recording.Columns.transcription.set(to: transcription),
                    Recording.Columns.progress.set(to: progress),
                    Recording.Columns.status.set(to: status.rawValue)
                ])
        }
        if updated > 0 {
            applyLocalProgressUpdate(id, transcription: transcription, progress: progress, status: status, isRegeneration: isRegeneration)
        }
    }

    func completeRecording(_ recording: Recording, transcription: String) async throws {
        guard recording.preparedPCMFileURL != nil || recording.queueArtifactState != nil else {
            try await completeRecordingTransaction(recording, transcription: transcription)
            return
        }
        try await queueArtifactLifecycle.perform {
            try await completeRecordingTransaction(recording, transcription: transcription)
        }
    }

    private func completeRecordingTransaction(_ recording: Recording, transcription: String) async throws {
        let updated = try await database().write { db -> Int in
            try Recording
                .filter(Recording.Columns.id == recording.id)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [
                    Recording.Columns.transcription.set(to: transcription),
                    Recording.Columns.progress.set(to: Float(1.0)),
                    Recording.Columns.status.set(to: RecordingStatus.completed.rawValue)
                ])
        }
        guard updated > 0 else { return }
        applyLocalProgressUpdate(
            recording.id,
            transcription: transcription,
            progress: 1.0,
            status: .completed,
            isRegeneration: false
        )
        try? await removePreparedPCMFileAndClearReference(for: recording)
    }

    func retryCompletedPreparedPCMCleanup() async throws {
        try await queueArtifactLifecycle.perform {
            try await retryCompletedPreparedPCMCleanupTransaction()
        }
    }

    private func retryCompletedPreparedPCMCleanupTransaction() async throws {
        let recordings = try await database().read { db in
            try Recording
                .filter(Recording.Columns.status == RecordingStatus.completed.rawValue)
                .filter(Recording.Columns.preparedPCMFileURL != nil)
                .filter(Recording.Columns.deletionState == nil)
                .fetchAll(db)
        }
        for recording in recordings {
            try? await removePreparedPCMFileAndClearReference(for: recording)
        }
    }

    func reconcileQueueArtifactLifecycle() async throws {
        try await queueArtifactLifecycle.perform {
            try await recoverStagedQueueArtifactsTransaction()
            try await retryCompletedPreparedPCMCleanupTransaction()
            try await retryDeletingRecordingsTransaction()
            if reconcilesOrphanedArtifacts {
                try await reconcileOrphanedQueueArtifactsTransaction(
                    artifactDirectory: QueuedPCMFile.directory
                )
            }
        }
    }

    func recoverStagedQueueArtifacts() async throws {
        try await queueArtifactLifecycle.perform {
            try await recoverStagedQueueArtifactsTransaction()
        }
    }

    private func recoverStagedQueueArtifactsTransaction() async throws {
        let staged = try await database().read { db in
            try Recording
                .filter(Recording.Columns.queueArtifactState == QueueArtifactState.staging.rawValue)
                .filter(Recording.Columns.deletionState == nil)
                .order(Recording.Columns.timestamp.asc)
                .fetchAll(db)
        }
        var firstError: Error?
        for recording in staged {
            do {
                try await recoverStagedQueueArtifact(recording)
            } catch {
                if firstError == nil { firstError = error }
            }
        }
        if let firstError { throw firstError }
    }

    private func recoverStagedQueueArtifact(_ recording: Recording) async throws {
        let fileManager = FileManager.default
        let durableSourceURL = recording.url
        if !fileManager.fileExists(atPath: durableSourceURL.path),
           let sourcePath = recording.sourceFileURL {
            let sourceURL = URL(fileURLWithPath: sourcePath)
            if fileManager.fileExists(atPath: sourceURL.path),
               sourceURL.standardizedFileURL != durableSourceURL.standardizedFileURL {
                try await moveQueueSource(from: sourceURL, to: durableSourceURL)
            }
        }

        guard fileManager.fileExists(atPath: durableSourceURL.path) else {
            if let path = recording.preparedPCMFileURL,
               fileManager.fileExists(atPath: path) {
                try fileManager.removeItem(atPath: path)
            }
            try await database().write { db in
                _ = try Recording
                    .filter(Recording.Columns.id == recording.id)
                    .filter(Recording.Columns.queueArtifactState == QueueArtifactState.staging.rawValue)
                    .deleteAll(db)
            }
            return
        }

        var recovered = recording
        recovered.sourceFileURL = durableSourceURL.path
        let preparedURL = recording.preparedPCMFileURL.map { URL(fileURLWithPath: $0) }
            ?? QueuedPCMFile.url(for: recording.id)
        if !fileManager.fileExists(atPath: preparedURL.path) {
            guard let samples = try await WhisperEngine().convertAudioToPCM(fileURL: durableSourceURL) else {
                throw TranscriptionError.audioConversionFailed
            }
            try await writePreparedPCM(samples, to: preparedURL, for: recording)
        }
        recovered.transcription = ""
        recovered.status = .pending
        recovered.preparedPCMFileURL = preparedURL.path
        try await activateStagedRecording(recovered)
    }

    func updateRecordingStatusOnly(_ id: UUID, progress: Float, status: RecordingStatus, isRegeneration: Bool? = nil) async throws {
        let updated = try await database().write { db -> Int in
            try Recording
                .filter(Recording.Columns.id == id)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [
                    Recording.Columns.progress.set(to: progress),
                    Recording.Columns.status.set(to: status.rawValue)
                ])
        }
        if updated > 0 {
            applyLocalProgressUpdate(id, progress: progress, status: status, isRegeneration: isRegeneration)
        }
    }

    func prepareRecordingForRequeue(_ recording: Recording, sourceURL: URL) async throws -> Bool {
        let transaction: () async throws -> Bool = {
            let updated = try await self.database().write { db -> Int in
                try Recording
                    .filter(Recording.Columns.id == recording.id)
                    .filter(Recording.Columns.deletionState == nil)
                    .updateAll(db, [
                        Recording.Columns.progress.set(to: Float(0)),
                        Recording.Columns.status.set(to: RecordingStatus.pending.rawValue),
                        Recording.Columns.sourceFileURL.set(to: sourceURL.path)
                    ])
            }
            if updated > 0 {
                self.applyLocalProgressUpdate(
                    recording.id,
                    progress: 0,
                    status: .pending,
                    isRegeneration: true
                )
            }
            return updated > 0
        }
        if recording.preparedPCMFileURL != nil || recording.queueArtifactState != nil {
            return try await queueArtifactLifecycle.perform(transaction)
        } else {
            return try await transaction()
        }
    }

    private nonisolated func updateRecordingInDB(_ recording: Recording) async throws {
        try await database().write { db in
            try Recording
                .filter(Recording.Columns.id == recording.id)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [
                    Recording.Columns.transcription.set(to: recording.transcription),
                    Recording.Columns.status.set(to: recording.status.rawValue),
                    Recording.Columns.progress.set(to: recording.progress),
                    Recording.Columns.sourceFileURL.set(to: recording.sourceFileURL),
                    Recording.Columns.preparedPCMFileURL.set(to: recording.preparedPCMFileURL),
                    Recording.Columns.queueArtifactState.set(to: recording.queueArtifactState?.rawValue)
                ])
        }
    }

    func deleteRecording(_ recording: Recording) {
        Task {
            do { try await deleteRecordingSync(recording) }
            catch { AppErrorCenter.shared.report("Recording could not be deleted", error: error) }
        }
    }

    func deleteRecordingSync(
        _ recording: Recording,
        cancelTranscription: Bool = true,
        cancelAndWait: ((UUID) async -> Void)? = nil
    ) async throws {
        let ownsQueueArtifact = recording.preparedPCMFileURL != nil || recording.queueArtifactState != nil
        let deletingRecording: Recording?
        if ownsQueueArtifact {
            deletingRecording = try await queueArtifactLifecycle.perform {
                let row = try await markRecordingsForDeletion([recording.id]).first
                if row != nil {
                    queueArtifactLifecycle.reserveDeletion(recording.id)
                }
                return row
            }
        } else {
            deletingRecording = try await markRecordingsForDeletion([recording.id]).first
        }
        guard let deletingRecording else { return }
        if cancelTranscription && recording.isPending {
            if let cancelAndWait {
                await cancelAndWait(recording.id)
            } else {
                await TranscriptionQueue.shared.cancelRecordingAndWait(recording.id)
            }
        }
        if ownsQueueArtifact {
            try await queueArtifactLifecycle.perform {
                defer {
                    queueArtifactLifecycle.releaseDeletion(recording.id)
                }
                try await finishDeletingRecording(deletingRecording)
            }
        } else {
            try await finishDeletingRecording(deletingRecording)
        }
    }

    func deleteAllRecordings() {
        Task {
            do { try await deleteAllRecordingsSync() }
            catch { AppErrorCenter.shared.report("Recordings could not be deleted", error: error) }
        }
    }

    func deleteAllRecordingsSync() async throws {
        await TranscriptionQueue.shared.stopProcessingQueue()
        try await queueArtifactLifecycle.perform {
            let ids = try await database().read { db in
                try Recording.fetchAll(db).map(\.id)
            }
            let recordings = try await markRecordingsForDeletion(ids)
            try await finishDeletingRecordings(recordings)
        }
    }

    nonisolated static func retentionCutoffDate(daysToKeep: Int, now: Date = Date()) -> Date? {
        guard daysToKeep > 0 else { return nil }
        return Calendar.current.date(byAdding: .day, value: -daysToKeep, to: now)
    }

    nonisolated static func isDeletableRecordingURL(_ url: URL) -> Bool {
        let recordingsPath = Recording.recordingsDirectory.standardizedFileURL.path
        let filePath = url.standardizedFileURL.path
        return filePath.hasPrefix(recordingsPath + "/") && filePath != recordingsPath
    }

    private nonisolated static let pendingStatuses = [
        RecordingStatus.pending.rawValue,
        RecordingStatus.converting.rawValue,
        RecordingStatus.transcribing.rawValue
    ]

    nonisolated func recordingsOlderThan(days: Int) async throws -> (count: Int, oldestDate: Date?) {
        guard let cutoff = Self.retentionCutoffDate(daysToKeep: days) else { return (0, nil) }
        return try await database().read { db in
            let request = Recording
                .filter(Recording.Columns.timestamp < cutoff)
                .filter(!Self.pendingStatuses.contains(Recording.Columns.status))
                .filter(Recording.Columns.queueArtifactState == nil)
                .filter(Recording.Columns.deletionState == nil)
            let count = try request.fetchCount(db)
            let oldest = try request
                .order(Recording.Columns.timestamp.asc)
                .limit(1)
                .fetchOne(db)
            return (count, oldest?.timestamp)
        }
    }

    func deleteRecordings(olderThanDays days: Int) async throws {
        guard let cutoff = Self.retentionCutoffDate(daysToKeep: days) else { return }
        try await queueArtifactLifecycle.perform {
            let outdated = try await database().read { db in
                try Recording
                    .filter(Recording.Columns.timestamp < cutoff)
                    .filter(!Self.pendingStatuses.contains(Recording.Columns.status))
                    .filter(Recording.Columns.queueArtifactState == nil)
                    .filter(Recording.Columns.deletionState == nil)
                    .fetchAll(db)
            }
            guard !outdated.isEmpty else { return }

            let ids = outdated.map(\.id)
            let recordings = try await markRecordingsForDeletion(ids)
            try await finishDeletingRecordings(recordings)
        }
    }

    nonisolated static func removePreparedPCMFile(for recording: Recording) throws {
        guard let path = recording.preparedPCMFileURL else { return }
        let url = URL(fileURLWithPath: path).standardizedFileURL
        let expected = QueuedPCMFile.url(for: recording.id).standardizedFileURL
        let legacy = recording.sourceFileURL.map {
            QueuedPCMFile.legacyURL(for: URL(fileURLWithPath: $0)).standardizedFileURL
        }
        guard url == expected || url == legacy else {
            throw TranscriptionError.audioConversionFailed
        }
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        try FileManager.default.removeItem(at: url)
    }

    private func removePreparedPCMFileAndClearReference(for recording: Recording) async throws {
        try Self.removePreparedPCMFile(for: recording)
        _ = try await database().write { db -> Int in
            try Recording
                .filter(Recording.Columns.id == recording.id)
                .filter(Recording.Columns.preparedPCMFileURL == recording.preparedPCMFileURL)
                .filter(Recording.Columns.deletionState == nil)
                .updateAll(db, [Recording.Columns.preparedPCMFileURL.set(to: nil)])
        }
        if let index = recordings.firstIndex(where: { $0.id == recording.id }) {
            recordings[index].preparedPCMFileURL = nil
        }
    }

    private func markRecordingsForDeletion(_ ids: [UUID]) async throws -> [Recording] {
        guard !ids.isEmpty else { return [] }
        let recordings = try await database().write { db in
            _ = try Recording
                .filter(ids.contains(Recording.Columns.id))
                .updateAll(db, [Recording.Columns.deletionState.set(to: RecordingDeletionState.deleting.rawValue)])
            return try Recording
                .filter(ids.contains(Recording.Columns.id))
                .fetchAll(db)
        }
        for recording in recordings {
            if let index = self.recordings.firstIndex(where: { $0.id == recording.id }) {
                self.recordings[index].deletionState = .deleting
            }
        }
        NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
        return recordings
    }

    private func finishDeletingRecording(_ recording: Recording) async throws {
        try Self.removePreparedPCMFile(for: recording)
        if FileManager.default.fileExists(atPath: recording.url.path) {
            try FileManager.default.removeItem(at: recording.url)
        }
        try await database().write { db in
            _ = try Recording
                .filter(Recording.Columns.id == recording.id)
                .filter(Recording.Columns.deletionState == RecordingDeletionState.deleting.rawValue)
                .deleteAll(db)
        }
        NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
    }

    private func finishDeletingRecordings(_ recordings: [Recording]) async throws {
        var firstError: Error?
        for recording in recordings {
            do {
                try await finishDeletingRecording(recording)
            } catch {
                if firstError == nil { firstError = error }
            }
        }
        if let firstError { throw firstError }
    }

    func retryDeletingRecordings() async throws {
        try await queueArtifactLifecycle.perform {
            try await retryDeletingRecordingsTransaction()
        }
    }

    private func retryDeletingRecordingsTransaction() async throws {
        let recordings = try await database().read { db in
            try Recording
                .filter(Recording.Columns.deletionState == RecordingDeletionState.deleting.rawValue)
                .fetchAll(db)
        }
        for recording in recordings {
            if queueArtifactLifecycle.deletionIsActive(recording.id) { continue }
            try? await finishDeletingRecording(recording)
        }
    }

    func reconcileOrphanedQueueArtifacts(
        artifactDirectory: URL = QueuedPCMFile.directory
    ) async throws {
        try await queueArtifactLifecycle.perform {
            try await reconcileOrphanedQueueArtifactsTransaction(artifactDirectory: artifactDirectory)
        }
    }

    private func reconcileOrphanedQueueArtifactsTransaction(
        artifactDirectory: URL
    ) async throws {
        let referencedPaths = Set(try await database().read { db in
            try Recording
                .filter(Recording.Columns.preparedPCMFileURL != nil)
                .fetchAll(db)
                .compactMap(\.preparedPCMFileURL)
        }.map { URL(fileURLWithPath: $0).standardizedFileURL.path })
        guard let artifacts = try? FileManager.default.contentsOfDirectory(
            at: artifactDirectory,
            includingPropertiesForKeys: nil
        ) else { return }

        for artifact in artifacts {
            let standardized = artifact.standardizedFileURL
            if referencedPaths.contains(standardized.path) { continue }
            try FileManager.default.removeItem(at: artifact)
        }
    }

    /// Recordings created through the indicator flow used to be saved with
    /// duration = 0. Restores real durations from the audio files on disk.
    nonisolated func backfillMissingDurations() async {
        let zeroDurationRecordings = (try? await database().read { db in
            try Recording
                .filter(Recording.Columns.duration <= 0)
                .filter(Recording.Columns.status == RecordingStatus.completed.rawValue)
                .filter(Recording.Columns.deletionState == nil)
                .fetchAll(db)
        }) ?? []
        guard !zeroDurationRecordings.isEmpty else { return }

        var updatedAny = false
        for recording in zeroDurationRecordings {
            guard FileManager.default.fileExists(atPath: recording.url.path) else { continue }
            let duration = await AudioUtil.audioDuration(url: recording.url)
            guard duration > 0 else { continue }
            do {
                try await database().write { db in
                    _ = try Recording
                        .filter(Recording.Columns.id == recording.id)
                        .updateAll(db, [Recording.Columns.duration.set(to: duration)])
                }
                updatedAny = true
            } catch {
                print("Failed to backfill duration for \(recording.id): \(error)")
            }
        }

        if updatedAny {
            await MainActor.run {
                NotificationCenter.default.post(name: Self.recordingsDidUpdateNotification, object: nil)
            }
        }
    }

    nonisolated static func recordingsDiskUsage(
        recordingsDirectory: URL = Recording.recordingsDirectory,
        preparedPCMDirectory: URL = QueuedPCMFile.directory
    ) -> Int64 {
        fileSize(in: recordingsDirectory) + fileSize(in: preparedPCMDirectory)
    }

    private nonisolated static func fileSize(in directory: URL) -> Int64 {
        let keys: Set<URLResourceKey> = [.isRegularFileKey, .fileSizeKey]
        guard let files = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: Array(keys)
        ) else { return 0 }
        return files.reduce(Int64(0)) { total, url in
            guard let values = try? url.resourceValues(forKeys: keys),
                  values.isRegularFile == true else { return total }
            return total + Int64(values.fileSize ?? 0)
        }
    }

    func latestSuccessfulTranscription() throws -> String? {
        try database().read { db in
            let cursor = try Recording
                .filter(Recording.Columns.status == RecordingStatus.completed.rawValue)
                .filter(Recording.Columns.deletionState == nil)
                .order(Recording.Columns.timestamp.desc)
                .fetchCursor(db)
            while let recording = try cursor.next() {
                if !recording.transcription.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    return recording.transcription
                }
            }
            return nil
        }
    }

    func searchRecordings(query: String) throws -> [Recording] {
        return try database().read { db in
            try Recording
                .filter(Recording.Columns.transcription.like("%\(query)%").collating(.nocase))
                .order(Recording.Columns.timestamp.desc)
                .limit(100)
                .fetchAll(db)
        }
    }
    
    nonisolated func searchRecordingsAsync(query: String, limit: Int = 100, offset: Int = 0) async throws -> [Recording] {
        return try await database().read { db in
            try Recording
                .filter(Recording.Columns.transcription.like("%\(query)%").collating(.nocase))
                .order(Recording.Columns.timestamp.desc)
                .limit(limit, offset: offset)
                .fetchAll(db)
        }
    }
}

struct PreservedAudioError: LocalizedError {
    let url: URL
    let underlying: Error
    var errorDescription: String? { "\(underlying.localizedDescription)\nAudio preserved at: \(url.path)" }
}

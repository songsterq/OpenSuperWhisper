import XCTest
import GRDB
import AVFoundation
@testable import OpenSuperWhisper

@MainActor
final class FailedAudioPreservationTests: XCTestCase {
    func testFailedDictationKeepsAudioAndCanBeRetried() async throws {
        let store = try RecordingStore(databaseQueue: DatabaseQueue())
        let source = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".wav")
        let bytes = Data([1, 2, 3, 4])
        try bytes.write(to: source)
        defer { try? FileManager.default.removeItem(at: source) }
        let row = try await store.saveFailedDictation(RecordedAudio(url: source, samples: [Float](repeating: 0, count: 16000)),
                                                       error: TranscriptionError.processingFailed)
        let preparedURL = URL(fileURLWithPath: try XCTUnwrap(row.preparedPCMFileURL))
        defer {
            try? FileManager.default.removeItem(at: row.url)
            try? FileManager.default.removeItem(at: preparedURL)
        }
        XCTAssertEqual(row.status, .failed)
        XCTAssertEqual(row.sourceFileURL, row.url.path)
        XCTAssertEqual(try Data(contentsOf: row.url), bytes)
        XCTAssertFalse(FileManager.default.fileExists(atPath: source.path))
        XCTAssertEqual(try QueuedPCMFile.read(from: preparedURL).count, 16000)
        let stored = try await store.fetchRecordings(limit: 10, offset: 0)
        XCTAssertEqual(stored.map(\.id), [row.id])
    }

    func testDatabaseFailureStillKeepsRecoverableAudio() async throws {
        let db = try DatabaseQueue()
        let store = try RecordingStore(databaseQueue: db)
        try await db.write { db in
            try db.execute(sql: "CREATE TRIGGER reject_insert BEFORE INSERT ON recordings BEGIN SELECT RAISE(ABORT, 'test refusal'); END")
        }
        let source = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".wav")
        try Data([7]).write(to: source)
        defer { try? FileManager.default.removeItem(at: source) }
        do {
            _ = try await store.saveFailedDictation(RecordedAudio(url: source, samples: []), error: TranscriptionError.processingFailed)
            XCTFail("Expected storage error")
        } catch let error as PreservedAudioError {
            defer { try? FileManager.default.removeItem(at: error.url) }
            XCTAssertEqual(try Data(contentsOf: error.url), Data([7]))
            XCTAssertTrue(error.localizedDescription.contains(error.url.path))
        }
    }

    func testClosingFailedPCMWriterPreservesAlreadyWrittenFrames() throws {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".wav")
        defer { try? FileManager.default.removeItem(at: url) }
        let format = try XCTUnwrap(AVAudioFormat(standardFormatWithSampleRate: 16000, channels: 1))
        let writer = try PCMRecordingWriter(url: url, inputFormat: format)
        let buffer = try XCTUnwrap(AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 16000))
        buffer.frameLength = 16000
        buffer.floatChannelData![0].initialize(repeating: 0.25, count: 16000)
        try writer.append(buffer)
        let audio = writer.closeAfterFailure()
        XCTAssertFalse(audio.samples.isEmpty)
        let file = try AVAudioFile(forReading: url)
        XCTAssertEqual(Int(file.length), audio.samples.count)
        XCTAssertGreaterThan(audio.duration, 0)
    }

    func testPreparedPCMLifecycleMatrix() async throws {
        enum TerminalEvent: CaseIterable {
            case failed
            case completed
            case deleted
            case interruptedCompletion
        }

        for event in TerminalEvent.allCases {
            let store = try RecordingStore(databaseQueue: DatabaseQueue())
            let source = FileManager.default.temporaryDirectory
                .appendingPathComponent("pcm-lifecycle-\(UUID()).wav")
            try Data([1, 2, 3]).write(to: source)
            let row = try await store.saveFailedDictation(
                RecordedAudio(url: source, samples: [0.25, -0.5, 0.75]),
                error: TranscriptionError.processingFailed
            )
            let preparedURL = URL(fileURLWithPath: try XCTUnwrap(row.preparedPCMFileURL))
            defer {
                try? FileManager.default.removeItem(at: row.url)
                try? FileManager.default.removeItem(at: preparedURL)
            }

            switch event {
            case .failed:
                break
            case .completed:
                try await store.completeRecording(row, transcription: "done")
            case .deleted:
                try await store.deleteRecordingSync(row, cancelTranscription: false)
            case .interruptedCompletion:
                try await store.updateRecordingStatusOnly(row.id, progress: 1, status: .completed)
                try FileManager.default.removeItem(at: preparedURL)
                try await store.retryCompletedPreparedPCMCleanup()
            }

            let persisted = try await store.fetchRecordings(limit: 1, offset: 0).first
            switch event {
            case .failed:
                XCTAssertEqual(persisted?.preparedPCMFileURL, preparedURL.path, "\(event)")
                XCTAssertTrue(FileManager.default.fileExists(atPath: preparedURL.path), "\(event)")
            case .completed, .interruptedCompletion:
                XCTAssertEqual(persisted?.status, .completed, "\(event)")
                XCTAssertNil(persisted?.preparedPCMFileURL, "\(event)")
                XCTAssertFalse(FileManager.default.fileExists(atPath: preparedURL.path), "\(event)")
            case .deleted:
                XCTAssertNil(persisted, "\(event)")
                XCTAssertFalse(FileManager.default.fileExists(atPath: preparedURL.path), "\(event)")
            }
        }
    }

    func testFailedDictationArtifactsSurviveTemporaryAgeCleanup() async throws {
        let store = try RecordingStore(databaseQueue: DatabaseQueue())
        let directory = AudioRecorder.temporaryRecordingsDirectory
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let source = directory.appendingPathComponent("pcm-age-\(UUID()).wav")
        try Data([4, 5, 6]).write(to: source)
        try FileManager.default.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: -(AudioRecorder.temporaryFileMaxAge + 60))],
            ofItemAtPath: source.path
        )
        try await store.addQueuedRecording(
            url: source,
            duration: 2.0 / 16000.0,
            pcmSamples: [0.125, -0.25]
        )
        let row = try XCTUnwrap(try store.getPendingRecordings().first)
        let preparedURL = URL(fileURLWithPath: try XCTUnwrap(row.preparedPCMFileURL))
        defer {
            try? FileManager.default.removeItem(at: row.url)
            try? FileManager.default.removeItem(at: preparedURL)
        }

        AudioRecorder.cleanupOldTemporaryFiles(
            in: directory,
            olderThan: AudioRecorder.temporaryFileMaxAge
        )

        XCTAssertTrue(FileManager.default.fileExists(atPath: row.url.path))
        XCTAssertTrue(FileManager.default.fileExists(atPath: preparedURL.path))
        XCTAssertEqual(try QueuedPCMFile.read(from: preparedURL), [0.125, -0.25])
    }

    func testCompletionRetainsPreparedPCMReferenceUntilDeletionCanFinish() async throws {
        let store = try RecordingStore(databaseQueue: DatabaseQueue())
        let source = FileManager.default.temporaryDirectory
            .appendingPathComponent("pcm-cleanup-retry-\(UUID()).wav")
        try Data([7, 8, 9]).write(to: source)
        let row = try await store.saveFailedDictation(
            RecordedAudio(url: source, samples: [0.5, -0.5]),
            error: TranscriptionError.processingFailed
        )
        let preparedURL = URL(fileURLWithPath: try XCTUnwrap(row.preparedPCMFileURL))
        defer {
            try? FileManager.default.setAttributes([.immutable: false], ofItemAtPath: preparedURL.path)
            try? FileManager.default.removeItem(at: preparedURL)
            try? FileManager.default.removeItem(at: row.url)
        }
        try FileManager.default.setAttributes([.immutable: true], ofItemAtPath: preparedURL.path)

        try await store.completeRecording(row, transcription: "done")

        var fetched = try await store.fetchRecordings(limit: 1, offset: 0)
        var persisted = try XCTUnwrap(fetched.first)
        XCTAssertEqual(persisted.status, .completed)
        XCTAssertEqual(persisted.preparedPCMFileURL, preparedURL.path)
        XCTAssertTrue(FileManager.default.fileExists(atPath: preparedURL.path))

        try FileManager.default.setAttributes([.immutable: false], ofItemAtPath: preparedURL.path)
        try await store.retryCompletedPreparedPCMCleanup()

        fetched = try await store.fetchRecordings(limit: 1, offset: 0)
        persisted = try XCTUnwrap(fetched.first)
        XCTAssertNil(persisted.preparedPCMFileURL)
        XCTAssertFalse(FileManager.default.fileExists(atPath: preparedURL.path))
    }
}

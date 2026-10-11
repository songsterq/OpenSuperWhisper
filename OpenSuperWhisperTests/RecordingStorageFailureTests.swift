import XCTest
import GRDB
@testable import OpenSuperWhisper

private enum LegacyRecordingStatus: String, Decodable, Equatable {
    case pending
    case converting
    case transcribing
    case completed
    case failed
}

private struct LegacyRecording: Decodable, FetchableRecord {
    let id: UUID
    let timestamp: Date
    let fileName: String
    let transcription: String
    let duration: TimeInterval
    let status: LegacyRecordingStatus
    let progress: Float
    let sourceFileURL: String?
}

@MainActor
final class RecordingStorageFailureTests: XCTestCase {
    func testUnavailableDatabaseReportsErrorInsteadOfCrashing() async throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data([1]).write(to: path)
        defer { try? FileManager.default.removeItem(at: path); AppErrorCenter.shared.issue = nil }
        let store = RecordingStore(databaseURL: path.appendingPathComponent("database.sqlite"))
        XCTAssertNotNil(AppErrorCenter.shared.issue)
        do {
            _ = try await store.fetchRecordings(limit: 10, offset: 0)
            XCTFail("Unavailable database was silently replaced")
        } catch {}
    }

    func testFailedDeletionLeavesRetryableTombstone() async throws {
        let db = try DatabaseQueue()
        let store = try RecordingStore(databaseQueue: db)
        let id = UUID()
        let row = Recording(id: id, timestamp: Date(), fileName: Recording.fileName(for: id),
                            transcription: "saved", duration: 1, status: .completed, progress: 1)
        try FileManager.default.createDirectory(at: Recording.recordingsDirectory, withIntermediateDirectories: true)
        try Data([1, 2, 3]).write(to: row.url)
        defer { try? FileManager.default.removeItem(at: row.url); AppErrorCenter.shared.issue = nil }
        try await store.addRecordingSync(row)
        try await db.write { db in
            try db.execute(sql: "CREATE TRIGGER reject_delete BEFORE DELETE ON recordings BEGIN SELECT RAISE(ABORT, 'test refusal'); END")
        }
        let vm = ContentViewModel(fetchPage: { _, _, _ in [] })
        vm.recordingStore = store
        vm.recordings = [row]
        AppErrorCenter.shared.issue = nil
        vm.deleteRecording(row)
        for _ in 0..<100 where AppErrorCenter.shared.issue == nil {
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTAssertNotNil(AppErrorCenter.shared.issue)
        XCTAssertEqual(vm.recordings.map(\.id), [id])
        XCTAssertFalse(FileManager.default.fileExists(atPath: row.url.path))
        do {
            try await store.deleteAllRecordingsSync()
            XCTFail("Deletion should fail")
        } catch {}
        let rows = try await store.fetchRecordings(limit: 10, offset: 0)
        XCTAssertEqual(rows.map(\.id), [id])
        XCTAssertEqual(rows.first?.status, .completed)
        XCTAssertEqual(rows.first?.deletionState, .deleting)
        let legacyRow = try await db.read { db in
            try LegacyRecording.fetchOne(
                db,
                sql: "SELECT * FROM recordings WHERE id = ?",
                arguments: [id]
            )
        }
        XCTAssertEqual(legacyRow?.id, id)
        XCTAssertEqual(legacyRow?.status, .completed)

        try await db.write { db in
            try db.execute(sql: "DROP TRIGGER reject_delete")
        }
        try await store.retryDeletingRecordings()
        let remaining = try await store.fetchRecordings(limit: 10, offset: 0)
        XCTAssertTrue(remaining.isEmpty)
    }

    func testFailedInsertIsReturnedToCaller() async throws {
        let db = try DatabaseQueue()
        let store = try RecordingStore(databaseQueue: db)
        try await db.write { db in
            try db.execute(sql: "CREATE TRIGGER reject_insert BEFORE INSERT ON recordings BEGIN SELECT RAISE(ABORT, 'test refusal'); END")
        }
        let id = UUID()
        let row = Recording(id: id, timestamp: Date(), fileName: Recording.fileName(for: id),
                            transcription: "text", duration: 1, status: .completed, progress: 1)
        do { try await store.addRecordingSync(row); XCTFail("Insert should fail") } catch {}
        let rows = try await store.fetchRecordings(limit: 10, offset: 0)
        XCTAssertTrue(rows.isEmpty)
    }
}

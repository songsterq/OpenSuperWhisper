import XCTest
@testable import OpenSuperWhisper

final class EmptyDictationDiscardTests: XCTestCase {

    private var dictationURL: URL {
        AudioRecorder.temporaryRecordingsDirectory.appendingPathComponent("12345.wav")
    }

    private var importedFileURL: URL {
        URL(fileURLWithPath: "/Users/user/Movies/interview.mp4")
    }

    func testEmptyTextFromDictation_isDiscarded() {
        XCTAssertTrue(TranscriptionQueue.shouldDiscardEmptyDictation(text: "", sourceURL: dictationURL))
    }

    func testEmptyTextFromImportedFile_isKept() {
        XCTAssertFalse(TranscriptionQueue.shouldDiscardEmptyDictation(text: "", sourceURL: importedFileURL))
    }

    func testEmptyTextFromDurableQueuedDictation_isDiscarded() {
        let durableURL = Recording.recordingsDirectory.appendingPathComponent("12345.wav")
        XCTAssertTrue(TranscriptionQueue.shouldDiscardEmptyDictation(
            text: "",
            sourceURL: durableURL,
            preparedPCMFileURL: QueuedPCMFile.url(for: UUID()).path
        ))
    }

    func testNonEmptyTextFromDictation_isKept() {
        XCTAssertFalse(TranscriptionQueue.shouldDiscardEmptyDictation(text: "hello world", sourceURL: dictationURL))
    }

    func testNonEmptyTextFromImportedFile_isKept() {
        XCTAssertFalse(TranscriptionQueue.shouldDiscardEmptyDictation(text: "hello world", sourceURL: importedFileURL))
    }
}

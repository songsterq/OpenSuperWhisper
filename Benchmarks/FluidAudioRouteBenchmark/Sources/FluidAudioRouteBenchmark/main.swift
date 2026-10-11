import CryptoKit
import FluidAudio
import Foundation

private struct RouteStatistics: Codable {
    let rawSeconds: [Double]
    let medianSeconds: Double
    let varianceSecondsSquared: Double
    let minimumSeconds: Double
    let maximumSeconds: Double
    let rangeSeconds: Double
    let realTimeFactor: Double
}

private struct BenchmarkReport: Codable {
    let generatedAt: String
    let operatingSystem: String
    let physicalMemoryBytes: UInt64
    let fluidAudioVersion: String
    let model: String
    let fixturePath: String
    let fixtureDurationSeconds: Double
    let fixtureSHA256: String
    let warmupsPerRoute: Int
    let measuredRunsPerRoute: Int
    let order: [String]
    let url: RouteStatistics
    let preparedPCM: RouteStatistics
    let urlToPCMSpeedup: Double
    let measuredTranscriptsMatched: Bool
    let methodology: String
    let interpretation: String
}

private enum BenchmarkError: Error, CustomStringConvertible {
    case usage
    case conversionFailed
    case unexpectedDuration(Double)
    case unexpectedFixtureHash(String)

    var description: String {
        switch self {
        case .usage:
            return "usage: FluidAudioRouteBenchmark FIXTURE OUTPUT_JSON"
        case .conversionFailed:
            return "could not prepare 16 kHz mono PCM"
        case .unexpectedDuration(let duration):
            return "fixture duration was \(duration), expected 97.106 seconds"
        case .unexpectedFixtureHash(let hash):
            return "fixture SHA-256 was \(hash), not the committed long_en fixture"
        }
    }
}

@main
private struct FluidAudioRouteBenchmark {
    static func main() async throws {
        guard CommandLine.arguments.count == 3 else { throw BenchmarkError.usage }
        let fixtureURL = URL(fileURLWithPath: CommandLine.arguments[1]).standardizedFileURL
        let outputURL = URL(fileURLWithPath: CommandLine.arguments[2]).standardizedFileURL
        let fixtureHash = SHA256.hash(data: try Data(contentsOf: fixtureURL))
            .map { String(format: "%02x", $0) }
            .joined()
        guard fixtureHash == "34c051c05fd076777903aae3694aef7386e9ad92126996cc3839655df1adf691" else {
            throw BenchmarkError.unexpectedFixtureHash(fixtureHash)
        }
        let samples = try preparePCM(from: fixtureURL)
        let duration = Double(samples.count) / 16000
        guard abs(duration - 97.106) < 0.001 else {
            throw BenchmarkError.unexpectedDuration(duration)
        }

        let models = try await AsrModels.downloadAndLoad(version: .v3)
        let manager = AsrManager(config: .default)
        try await manager.loadModels(models)

        _ = try await transcribeURL(fixtureURL, manager: manager)
        _ = try await transcribePCM(samples, manager: manager)

        var urlTimes: [Double] = []
        var pcmTimes: [Double] = []
        var urlTranscripts: [String] = []
        var pcmTranscripts: [String] = []
        var order: [String] = []

        for _ in 0..<5 {
            var start = DispatchTime.now().uptimeNanoseconds
            let urlText = try await transcribeURL(fixtureURL, manager: manager)
            var end = DispatchTime.now().uptimeNanoseconds
            urlTimes.append(Double(end - start) / 1_000_000_000)
            urlTranscripts.append(urlText)
            order.append("url")

            start = DispatchTime.now().uptimeNanoseconds
            let pcmText = try await transcribePCM(samples, manager: manager)
            end = DispatchTime.now().uptimeNanoseconds
            pcmTimes.append(Double(end - start) / 1_000_000_000)
            pcmTranscripts.append(pcmText)
            order.append("prepared_pcm")
        }

        let urlStats = statistics(urlTimes, duration: duration)
        let pcmStats = statistics(pcmTimes, duration: duration)
        let report = BenchmarkReport(
            generatedAt: ISO8601DateFormatter().string(from: Date()),
            operatingSystem: ProcessInfo.processInfo.operatingSystemVersionString,
            physicalMemoryBytes: ProcessInfo.processInfo.physicalMemory,
            fluidAudioVersion: "0.15.6",
            model: "Parakeet TDT 0.6B v3",
            fixturePath: fixtureURL.path,
            fixtureDurationSeconds: duration,
            fixtureSHA256: fixtureHash,
            warmupsPerRoute: 1,
            measuredRunsPerRoute: 5,
            order: order,
            url: urlStats,
            preparedPCM: pcmStats,
            urlToPCMSpeedup: urlStats.medianSeconds / pcmStats.medianSeconds,
            measuredTranscriptsMatched: Set(urlTranscripts + pcmTranscripts).count == 1,
            methodology: "One process and one loaded AsrManager; a fresh decoder state per utterance; one URL and one prepared-PCM warmup; five measured URL/PCM pairs in alternating order.",
            interpretation: "This isolates file decoding, conversion, and input-routing overhead before the same Parakeet v3 model/runtime. It excludes model loading and does not compare Parakeet with Whisper."
        )

        try FileManager.default.createDirectory(
            at: outputURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        let data = try encoder.encode(report)
        try data.write(to: outputURL, options: .atomic)
        FileHandle.standardOutput.write(data)
        FileHandle.standardOutput.write(Data([0x0A]))
    }

    private static func transcribeURL(_ url: URL, manager: AsrManager) async throws -> String {
        var state = try TdtDecoderState(decoderLayers: await manager.decoderLayerCount)
        return try await manager.transcribe(url, decoderState: &state).text
    }

    private static func transcribePCM(_ samples: [Float], manager: AsrManager) async throws -> String {
        var state = try TdtDecoderState(decoderLayers: await manager.decoderLayerCount)
        return try await manager.transcribe(samples, decoderState: &state).text
    }

    private static func statistics(_ values: [Double], duration: Double) -> RouteStatistics {
        let sorted = values.sorted()
        let median = sorted[sorted.count / 2]
        let mean = values.reduce(0, +) / Double(values.count)
        let variance = values.reduce(0) { $0 + ($1 - mean) * ($1 - mean) } / Double(values.count)
        let minimum = sorted.first!
        let maximum = sorted.last!
        return RouteStatistics(
            rawSeconds: values,
            medianSeconds: median,
            varianceSecondsSquared: variance,
            minimumSeconds: minimum,
            maximumSeconds: maximum,
            rangeSeconds: maximum - minimum,
            realTimeFactor: median / duration
        )
    }

    private static func preparePCM(from url: URL) throws -> [Float] {
        try AudioConverter().resampleAudioFile(url)
    }
}

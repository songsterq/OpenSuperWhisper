# FluidAudio URL versus prepared-PCM benchmark

This benchmark compares FluidAudio 0.15.6 / Parakeet v3 routes while holding
the model, loaded manager, fixture, decoder-state lifecycle, and process
constant. It performs one warmup per route, then five alternating URL and
prepared 16 kHz mono `[Float]` measurements.

```sh
swift run -c release FluidAudioRouteBenchmark \
  ../../OpenSuperWhisperTests/Fixtures/long_en.m4a \
  results/parakeet-v3-url-vs-pcm.json
```

The URL result includes FluidAudio's file decoding, conversion, and routing
overhead. The PCM result begins after equivalent samples have been prepared.
Neither route comparison measures a model substitution or model loading.

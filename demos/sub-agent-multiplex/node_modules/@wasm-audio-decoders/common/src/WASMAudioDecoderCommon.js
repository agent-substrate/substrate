import { decode } from "simple-yenc";

export default function WASMAudioDecoderCommon() {
  // setup static methods
  const uint8Array = Uint8Array;
  const float32Array = Float32Array;

  if (!WASMAudioDecoderCommon.modules) {
    Object.defineProperties(WASMAudioDecoderCommon, {
      modules: {
        value: new WeakMap(),
      },

      setModule: {
        value(Ref, module) {
          WASMAudioDecoderCommon.modules.set(Ref, Promise.resolve(module));
        },
      },

      getModule: {
        value(Ref, wasmString) {
          let module = WASMAudioDecoderCommon.modules.get(Ref);

          if (!module) {
            if (!wasmString) {
              wasmString = Ref.wasm;
              module = WASMAudioDecoderCommon.inflateDynEncodeString(
                wasmString,
              ).then((data) => WebAssembly.compile(data));
            } else {
              module = WebAssembly.compile(decode(wasmString));
            }

            WASMAudioDecoderCommon.modules.set(Ref, module);
          }

          return module;
        },
      },

      concatFloat32: {
        value(buffers, length) {
          let ret = new float32Array(length),
            i = 0,
            offset = 0;

          while (i < buffers.length) {
            ret.set(buffers[i], offset);
            offset += buffers[i++].length;
          }

          return ret;
        },
      },

      getDecodedAudio: {
        value: (errors, channelData, samplesDecoded, sampleRate, bitDepth) => ({
          errors,
          channelData,
          samplesDecoded,
          sampleRate,
          bitDepth,
        }),
      },

      getDecodedAudioMultiChannel: {
        value(
          errors,
          input,
          channelsDecoded,
          samplesDecoded,
          sampleRate,
          bitDepth,
        ) {
          let channelData = [],
            i,
            j;

          for (i = 0; i < channelsDecoded; i++) {
            const channel = [];
            for (j = 0; j < input.length; ) channel.push(input[j++][i] || []);
            channelData.push(
              WASMAudioDecoderCommon.concatFloat32(channel, samplesDecoded),
            );
          }

          return WASMAudioDecoderCommon.getDecodedAudio(
            errors,
            channelData,
            samplesDecoded,
            sampleRate,
            bitDepth,
          );
        },
      },

      /*
       ******************
       * Compression Code
       ******************
       */

      inflateDynEncodeString: {
        value(source) {
          source = decode(source);

          return new Promise((resolve) => {
            // prettier-ignore
            const puffString = String.raw`dynEncode012804c7886d()((()>+*§§)§,§§§§)§+§§§)§+.-()(*)-+)(.7*§)i¸¸,3§(i¸¸,3/G+.¡*(,(,3+)2å:-),§H(P*DI*H(P*@I++hH)H*r,hH(H(P*<J,i)^*<H,H(P*4U((I-H(H*i0J,^*DH+H-H*I+H,I*4)33H(H*H)^*DH(H+H)^*@H+i§H)i§3æ*).§K(iHI/+§H,iHn,§H+i(H+i(rCJ0I,H*I-+hH,,hH(H-V)(i)J.H.W)(i)c)(H,i)I,H-i*I-4)33i(I.*hH(V)(H+n5(H(i*I-i(I,i)I.+hH,i*J+iHn,hi(I-i*I,+hH,H/H-c)(H,iFn,hi(I,+hH,H0n5-H*V)(J(,hH/H(i)J(H(V)(J(i)c)(H)H(i)H,c)(3H*i*I*H,i)I,4(3(-H(H,W)(H-I-H,i*I,4)3(3(3H,H-I1H+I,H.i)H1V)(J.i(v5(33H.-H(H,i(c)(H,i*I,4)333)-§i*I*+§H*iHn,hi73H,H(i)8(H+J+H)P*(H*V)(J-r,§H)P*,H.i)H+H,i)V)(-H*i*I*H+i)I+H-H.I.H,H-i)I,4)333Ã+)-§iø7i(^*(iü7I,*h+hH+iDn,h*hilI+i)I,+hH+,hH+iô7H,c)(i)H+i´8W)(H,I,H+i*I+4)-+hH(H)8*J-i(p5.*h*h*hH-i')u,hH(P*(J+,hH(P*0J,H(P*,n50H+H,H-b((3H(P*0i)I.4)3H-i¨*n5*H-iÅ*s,hi73H-i)J+V)&+I,H(H+V)æ,8(I.H(H*8*J-i(p51H-i)J+i¸7V)(H(H+iø7V)(8(J/H(P*0J+s,hi73H+H,H.J,I.H(P*(m5(H.H(P*,s5.+hH,m5*H(P*(J.H+H.H+H/U((b((H(H(P*0i)J+^*0H,i)I,4(3(3H(H.^*03H-i¨*o5)33i(73(3(3-H,H+i)c)(H,i*I,H+i)I+4)33i)I-3H-3!2)0§K(i2J,L(H,H(^*(H,H*^*4H,i(^*0H,i(^*DH,j(_*<H,H)P*(^*,H,H+P*(^*8*h*h+hH,i)8(I3i§I**h*h*h*h*h*h*hH,i*8(6+(),03H,j(_*@i*I-H,P*<J.i,J(H,P*8J/s50H,H.i+J0^*<i¦I*H.H,P*4J1J.U(*H.U((J2i')o5/H.U()I.H,H(^*<H0H1U((H.i0J.i§i0i')o5/H/H.H2J*H(J.q50H,P*0J/H*I-H,P*(J0,hH,P*,H-q,hi)I-423+hH*m5+H/H0H(H1U((b((H/i)I/H(i)I(H*i)I*4(3(3H,H.^*<H,H-^*04*3iØ1U((5+i(I(i¨7i1^*(i$6iè1^*(i°7iè6^*(i¬7iÈ6^*(+hH(iÈ*n,hiÈ*I(+hH(i¨,n,hi¨,I(+hH(iØ,n,hiØ,I(+hH(iè,o,hH,i-H(i0c)(H(i*I(4)33iè1i1H,i-iÈ*8)Bi(I(+hH(ido,hH,i-H(i-c)(H(i*I(4)33iÈ6iè6H,i-iF8)BiØ1i)b((41-H,i-H(i/c)(H(i*I(4)3(3(-H,i-H(i1c)(H(i*I(4)3(3(-H,i-H(i0c)(H(i*I(4)3(3(3H,H/^*0H,H(^*<3i(I*4*3H,H,i¸)^*TH,H,iø-^*PH,H,iX^*LH,H,i(^*HH,i-8(I(H,i-8(I-i¥I*H,i,8(I.H(iErH-iEr5)H(i©*I1H-i)I0i(i;H.i,J(i(H(i(rCJ(J*H*i;sCI*i¨1I-H(I/+hH/,hH,i-H-V)(i)H,i+8(c)(H/i)I/H-i*I-H*i)I*4)-H(i)i¨1I/+hH(H*o,hH,i-H/V)(i)i(c)(H/i*I/H(i)I(4)33i¤I*H,iø-H,i¸)H,i-i;8)5+H0H1I2i(I-+hH-H2p,hH,H,iP8*J*i(p5-H*i7u,hH,i-H-i)H*c)(H-i)I-4*3i(I/i+I.i+I(*h*h*hH*i86*(*)3H-m,hi£I*403H-i)H,W)-I/i*I(4)3i3I.i/I(3H2H,H(8(H.J(H-J.p,hi¢I*4.3H,i-H-i)I*+hH(,hH*H/c)(H*i*I*H(i)I(4)-H.I-4+3(3(33H,W)1m,hiI*4,3H,iø-H,i¸)H,i-H18)J(,hi¡I*H(i(p5,H1H,V)ú-H,V)ø-o5,3H,i(H,iXH,i-H1i)H08)J(,hi I*H(i(p5,H0H,V)H,V)o5,3H,H,iPH,iH8+I*4+3(3(3H,i$6i¬78+I*3H*H3m5(3i)I-H*i(r5)3H)H,P*0^*(H+H,P*<^*(H*I-3H,i2L(H-33Á)+(i¨03b+(,(-(.(/(0(1(2(3(5(7(9(;(?(C(G(K(S([(c(k({(((«(Ë(ë((*)(iø03O)()()()(*(*(*(*(+(+(+(+(,(,(,(,(-(-(-(-(i¨13M8(9(:(((0(/(1(.(2(-(3(,(4(+(5(*(6()(7(T7*S7US0U `;

            WASMAudioDecoderCommon.getModule(WASMAudioDecoderCommon, puffString)
              .then((wasm) => WebAssembly.instantiate(wasm, {}))
              .then(({ exports }) => {
                // required for minifiers that mangle the __heap_base property
                const instanceExports = new Map(Object.entries(exports));

                const puff = instanceExports.get("puff");
                const memory = instanceExports.get("memory")["buffer"];
                const dataArray = new uint8Array(memory);
                const heapView = new DataView(memory);

                let heapPos = instanceExports.get("__heap_base");

                // source length
                const sourceLength = source.length;
                const sourceLengthPtr = heapPos;
                heapPos += 4;
                heapView.setInt32(sourceLengthPtr, sourceLength, true);

                // source data
                const sourcePtr = heapPos;
                heapPos += sourceLength;
                dataArray.set(source, sourcePtr);

                // destination length
                const destLengthPtr = heapPos;
                heapPos += 4;
                heapView.setInt32(
                  destLengthPtr,
                  dataArray.byteLength - heapPos,
                  true,
                );

                // destination data fills in the rest of the heap
                puff(heapPos, destLengthPtr, sourcePtr, sourceLengthPtr);

                resolve(
                  dataArray.slice(
                    heapPos,
                    heapPos + heapView.getInt32(destLengthPtr, true),
                  ),
                );
              });
          });
        },
      },
    });
  }

  Object.defineProperty(this, "wasm", {
    enumerable: true,
    get: () => this._wasm,
  });

  this.getOutputChannels = (outputData, channelsDecoded, samplesDecoded) => {
    let output = [],
      i = 0;

    while (i < channelsDecoded)
      output.push(
        outputData.slice(
          i * samplesDecoded,
          i++ * samplesDecoded + samplesDecoded,
        ),
      );

    return output;
  };

  this.allocateTypedArray = (len, TypedArray, setPointer = true) => {
    const ptr = this._wasm.malloc(TypedArray.BYTES_PER_ELEMENT * len);
    if (setPointer) this._pointers.add(ptr);

    return {
      ptr: ptr,
      len: len,
      buf: new TypedArray(this._wasm.HEAP, ptr, len),
    };
  };

  this.free = () => {
    this._pointers.forEach((ptr) => {
      this._wasm.free(ptr);
    });
    this._pointers.clear();
  };

  this.codeToString = (ptr) => {
    const characters = [],
      heap = new Uint8Array(this._wasm.HEAP);
    for (let character = heap[ptr]; character !== 0; character = heap[++ptr])
      characters.push(character);

    return String.fromCharCode.apply(null, characters);
  };

  this.addError = (
    errors,
    message,
    frameLength,
    frameNumber,
    inputBytes,
    outputSamples,
  ) => {
    errors.push({
      message: message,
      frameLength: frameLength,
      frameNumber: frameNumber,
      inputBytes: inputBytes,
      outputSamples: outputSamples,
    });
  };

  this.instantiate = (_EmscriptenWASM, _module) => {
    if (_module) WASMAudioDecoderCommon.setModule(_EmscriptenWASM, _module);
    this._wasm = new _EmscriptenWASM(WASMAudioDecoderCommon).instantiate();
    this._pointers = new Set();

    return this._wasm.ready.then(() => this);
  };
}

/**
 * Web-Audio-API decoder
 * @module  audio-decode
 */

import getType from 'audio-type';
import AudioBufferShim from 'audio-buffer';

const AudioBuffer = globalThis.AudioBuffer || AudioBufferShim;

/**
 * Decode an audio buffer.
 *
 * @param {ArrayBuffer | Uint8Array} buf - The audio data to decode.
 * @returns {Promise<AudioBuffer>} A promise that resolves to the decoded audio buffer.
 * @throws {Error} Throws an error if the decode target is invalid or if the audio format is not supported.
 */
export default async function audioDecode(buf) {
	if (!buf && !(buf.length || buf.buffer)) throw Error('Bad decode target')
	buf = new Uint8Array(buf.buffer || buf)

	let type = getType(buf);

	if (!type) throw Error('Cannot detect audio format');

	if (!decoders[type]) throw Error('Missing decoder for ' + type + ' format')

	return decoders[type](buf)
};

export const decoders = {
	async oga(buf) {
		let { decoder } = decoders.oga
		if (!decoder) {
			let { OggVorbisDecoder } = await import('@wasm-audio-decoders/ogg-vorbis')
			await (decoders.oga.decoder = decoder = new OggVorbisDecoder()).ready;
		} else await decoder.reset()
		return buf && createBuffer(await decoder.decodeFile(buf))
	},
	async mp3(buf) {
		let { decoder } = decoders.mp3
		if (!decoder) {
			const { MPEGDecoder } = await import('mpg123-decoder')
			await (decoders.mp3.decoder = decoder = new MPEGDecoder()).ready;
		}
		else await decoder.reset()
		return buf && createBuffer(await decoder.decode(buf))
	},
	async flac(buf) {
		let { decoder } = decoders.flac
		if (!decoder) {
			const { FLACDecoder } = await import('@wasm-audio-decoders/flac')
			await (decoders.flac.decoder = decoder = new FLACDecoder()).ready
		}
		else await decoder.reset()
		return buf && createBuffer(await decoder.decode(buf))
	},
	async opus(buf) {
		let { decoder } = decoders.opus
		if (!decoder) {
			const { OggOpusDecoder } = await import('ogg-opus-decoder')
			await (decoders.opus.decoder = decoder = new OggOpusDecoder()).ready
		}
		else await decoder.reset()
		return buf && createBuffer(await decoder.decodeFile(buf))
	},
	async wav(buf) {
		let { decode } = decoders.wav
		if (!decode) {
			let module = await import('node-wav')
			decode = decoders.wav.decode = module.default.decode
		}
		return buf && createBuffer(await decode(buf))
	},
	async qoa(buf) {
		let { decode } = decoders.qoa
		if (!decode) {
			decoders.qoa.decode = decode = (await import('qoa-format')).decode
		}
		return buf && createBuffer(await decode(buf))
	}
}

function createBuffer({ channelData, sampleRate }) {
	let audioBuffer = new AudioBuffer({
		sampleRate,
		length: channelData[0].length,
		numberOfChannels: channelData.length
	})
	for (let ch = 0; ch < channelData.length; ch++) audioBuffer.getChannelData(ch).set(channelData[ch])
	return audioBuffer
}

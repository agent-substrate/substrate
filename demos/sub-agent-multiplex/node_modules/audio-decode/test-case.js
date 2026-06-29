import fs from 'fs/promises'

async function main(options) {
    const audioDecode = await import('./audio-decode.js');
    const decode = audioDecode.default;
    await audioDecode.decoders.mp3();
    const buffer = await fs.readFile('test.mp3');
    const decodedBuffer = await decode(buffer);
}

main().catch(console.error)
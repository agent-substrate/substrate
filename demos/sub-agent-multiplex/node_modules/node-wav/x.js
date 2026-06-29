let fs = require('fs');
let wav = require('./index');

//let files = JSON.parse(fs.readFileSync('x.json'));

let files = ["../train/data//other/0005932e-db17-4a21-b99f-5e10f84a9e74.wav"];

files.forEach(file => {
  console.log(file);
  let buffer = fs.readFileSync(file);
  let w = wav.decode(buffer);  
});

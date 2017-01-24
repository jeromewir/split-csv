const firstlinereader = require('firstline');
const Liner = require('liner');
const path = require('path');
const fs = require('fs');
const async = require('async');
const prependFile = require('prepend-file');
const splitFile = require('split-file');
const mkdirp = require('mkdirp');
const readline = require('readline');

function countLines(filePath) {
  return new Promise((resolve, reject) => {
    let count = 0;
    fs.createReadStream(filePath)
      .on('data', chunk => {
        for (let i = 0; i < chunk.length; ++i)
          if (chunk[i] == 10)++count;
      })
      .on('error', reject)
      .on('end', () => resolve(count));
  });
}

function handleSplitByLines(filePath, options) {
  return new Promise((resolve, reject) => {
    const liner = new Liner(filePath);
    const nbLines = options.nbLines;
    const files = [];
    const baseName = path.parse(filePath).name;
    let curLine = -1;
    let ended = false;
    const queue = async.queue(({ stream, line }, next) => stream.write(`${line}\n`, next));
    let header = null;

    queue.drain = () => {
      if (ended) return resolve(files);
    };

    function getNewFile() {
      const newfilename = `${baseName}-part${files.length}.csv`;
      files.push(newfilename);
      return fs.createWriteStream(path.join(options.dest || '.', newfilename));
    }
    let stream = getNewFile();

    const lineReader = readline.createInterface({
      input: fs.createReadStream(filePath)
    });

    lineReader.on('line', line => {
      if (!header) header = line;
      if (curLine >= nbLines) {
        stream = getNewFile();
        curLine = 0;
        queue.push({ stream, line: header });
      }
      queue.push({ stream, line });
      ++curLine;
    });

    lineReader.on('error', reject);
    lineReader.on('close', () => ended = true);
  });
}

function splitByLines(filePath, options = {}) {
  return new Promise((resolve, reject) => {
    if (options.dest) {
      mkdirp(options.dest, (err) => {
        if (err) return reject(err);
        return handleSplitByLines(filePath, options)
          .then(resolve).catch(reject);
      });
    } else {
      return handleSplitByLines(filePath, options)
        .then(resolve).catch(reject);
    }
  });
}

function splitByPacket(filePath, options = {}) {
  return new Promise((resolve, reject) => {
    if (!options.nbPackets) return reject('nbPackets is mandatory');
    countLines(filePath).then((nbLines) => {
      const packetSize = Math.ceil(nbLines / options.nbPackets);
      if (packetSize < 1) return reject('Packets too small for file');
      options = Object.assign(options, { nbLines: packetSize });
      if (options.dest) {
        mkdirp(options.dest, (err) => {
          if (err) return reject(err);
          return splitByLines(filePath, options)
            .then(resolve).catch(reject);
        });
      } else {
        return splitByLines(filePath, options)
          .then(resolve).catch(reject);
      }
    }).catch(reject);
  });
}

module.exports = {
  splitByPacket, splitByLines
};
const splitCsv = require('../lib/index.js');

splitCsv.splitByLines('files/smallFile.csv', { dest: 'dest', nbLines: 100 })
  .then((files) => {
    console.log(files);
  }).catch(err => console.error(err.stack));
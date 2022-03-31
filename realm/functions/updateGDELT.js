exports = async function () {
  const AdmZip = require("adm-zip");
  
  const db = context.services.get("gdelt").db("recentEvents");
  const insertLog = db.collection("insertLog");
  
  const lastUpdate = (await context.http.get({ url: 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt' })).body.text();
  const match = lastUpdate.match(/https?:\/\/data.gdeltproject.org\/gdeltv2\/(\d+).export.CSV.zip/);
  const csvURL = match[0];
  const downloadId = match[1];
  
  console.log("downloadId: ", downloadId);
  if (await insertLog.findOne({ _id: downloadId }) !== null) {
    console.log(`${downloadId} already inserted.`)
    return null;
  }

  const latestCSV = (await context.http.get({ url: csvURL })).body;
  const zip = new AdmZip(new Buffer(latestCSV.toBase64(), 'base64'));
  const lines = zip.getEntries()[0].getData().toString("utf8").split("\n");
  // There's a trailing \n that results in an extra row being added.
  // It needs to be removed:
  if (lines[lines.length - 1] === "") {
    lines.pop();
  }
  const rows = lines.map((line) => line.split("\t"));
  const insertResult = await context.functions.execute('insertCSVRows', rows, downloadId);
  
  await insertLog.insertOne({
    _id: downloadId,
  });
  
  return insertResult;
};

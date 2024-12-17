c = db.downloadedDocuments.files.find({},{_id:0, filename:1, place_filename:1})
while (c.hasNext()) {
	d = c.next();
	printjson(d);
}

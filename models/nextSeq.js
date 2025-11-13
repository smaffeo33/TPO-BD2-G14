const mongoose = require('mongoose');


async function nextSeq(key) {
    const db = mongoose.connection?.db;
    if (!db) throw new Error('MongoDB not connected yet');

    const counters = db.collection('counters');

    await counters.updateOne(
        { _id: key },
        { $setOnInsert: { seq: 0 } },
        { upsert: true }
    );

    let res;
    try {
        res = await counters.findOneAndUpdate(
            { _id: key },
            { $inc: { seq: 1 } },
            { upsert: true, returnDocument: 'after', projection: { seq: 1 } }
        );
    } catch {
        res = await counters.findOneAndUpdate(
            { _id: key },
            { $inc: { seq: 1 } },
            { upsert: true, returnOriginal: false, projection: { seq: 1 } }
        );
    }

    const doc = res?.value || await counters.findOne({ _id: key }, { projection: { seq: 1 } });
    if (!doc || typeof doc.seq !== 'number') {
        throw new Error(`Failed to obtain next sequence for '${key}'`);
    }
    return doc.seq;
}

module.exports = nextSeq;
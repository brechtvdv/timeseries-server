const MultidimensionalInterface = require('../../lib/MultidimensionalInterface');
const Utils = require('../../lib/Utils');
const md5 = require('md5');
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;

class RawData extends MultidimensionalInterface {

    constructor(config, commMan) {
        super(commMan);
        this._serverUrl = super.commMan.config.serverUrl;
        this._name = config.name;
        this._baseUri = this._serverUrl + this._name + '/fragments';
        this._websocket = config.websocket;
        this._fragmentsPath = config.fragmentsPath;
        this._fragmentMaxSize = config.maxFileSize;
        this._byteCounter = 0;
        this._latestWindowsize = config.latestWindowsize; // how many observations in /latest
        this._latest = [];
        this._latestTrig = ''; // String of all latest triples in TriG
        this._staticTriples = ''; // TriG string
        this._prevEtag = '';
        this._prevResponseBody = '';
        this._license = config.license;

        this.setupStaticTriples(config.staticTriples);

        // Load HTTP interfaces for this interface
        this.setupPollInterfaces();

        // Load Websocket interface
        if (this.websocket) {
            super.setupPubsupInterface(this.name, config.wsPort);
        }

        // Init storage folder
        Utils.createFolder(this.fragmentsPath);
    }

    async setupStaticTriples(path) {
        let st = await Utils.getTriplesFromFile(path);
        if (st && st[1]) this._staticTriples = await Utils.formatTriples('application/trig', st[1], st[0]);
    }

    onData(data) {
        // Multiple observations received as array
        if (Array.isArray(data)) {
            data.forEach((_data) => {
                this.init(_data);
            })
        } else {
            this.init(data);
        }
    }

    async init(data) {
        const gat = await Utils.getGeneratedAtTimeValue(data);
        const triples = await Utils.getTriplesFromString(data);
        const triplesTrig = await Utils.formatTriples('application/trig', triples[1]);

        this._latest.push({
            'data': data,
            'triples': triples[1],
            'triplesTrig': triplesTrig,
            'gat': gat
        })

        // If applicable push data to subscribed clients through Websocket
        if (this.websocket) {
            super.commMan.pushData(this.name, this._staticTriples.concat(data.toString()));
        }

        // When window of latest data is full, store oldest in fragment
        if (this._latest.length > this._latestWindowsize) {
            const o = this._latest.pop();
            this.storeData(o.data, o.gat);
        }

        let newLatestTrig = '';
        for (let i = 0; i < this._latest.length; i++) {
           newLatestTrig += '\n' + this._latest[i].triplesTrig;
        }
        this._latestTrig = newLatestTrig;
    }

    setupPollInterfaces() {
        let self = this;

        // HTTP interface to get the latest data update
        super.commMan.router.get('/' + this.name + '/latest', async (ctx, next) => {
            ctx.response.set({ 'Access-Control-Allow-Origin': '*' });

            if (self._latest.length === 0) {
                ctx.response.status = 404;
                ctx.response.body = "No data found";
            } else {
                let etag = 'W/"' + md5(self._latest[self._latest.length-1].data) + '"'; // MD5 of last observation
                let ifNoneMatchHeader = ctx.request.header['if-none-match'];
                let last_modified = self._latest[self._latest.length-1].gat.toUTCString();

                if (ifNoneMatchHeader && ifNoneMatchHeader === etag) {
                    ctx.response.status = 304;
                } else if (etag === this._prevEtag) {
                    return prevResponse;
                } else {
                    ctx.response.set({
                        //'Cache-Control': 'public, s-maxage=' + (maxage - 1) + ', max-age=' + maxage + ', must-revalidate',
                        //'Expires': expires,
                        'ETag': etag,
                        'Last-Modified': last_modified,
                        'Content-Type': 'application/trig'
                    });

                    // Hydra controls
                    let uri = this._baseUri + '/latest';
                    let index = Utils.getAllFragments(this.fragmentsPath).length; // latest fragment
                    let metadata = await this.createMetadata(uri, index);
                    this._prevResponseBody = this._staticTriples.concat('\n' + this._latestTrig, '\n' + metadata);
                    this._prevEtag = etag;
                    ctx.response.body = this._prevResponseBody;
                }
            }
        });

        // HTTP interface to get a specific fragment of data (historic data)
        super.commMan.router.get('/' + this.name + '/fragments', async (ctx, next) => {
            ctx.response.set({ 'Access-Control-Allow-Origin': '*' });
            let queryTime = new Date(ctx.query.time);

            if (queryTime.toString() === 'Invalid Date') {
                // Redirect to now time
                ctx.status = 302
                ctx.redirect('/' + this.name + '/fragments?time=' + new Date().toISOString());
                return;
            }

            let fragments = Utils.getAllFragments(this.fragmentsPath).map(f => new Date(f.substring(0, f.indexOf('.trig'))).getTime());
            let [fragment, index] = Utils.dateBinarySearch(queryTime.getTime(), fragments);

            if (queryTime.getTime() !== fragment.getTime()) {
                // Redirect to correct fragment URL
                ctx.status = 302
                ctx.redirect('/' + this.name + '/fragments?time=' + fragment.toISOString());
                return;
            }

            let fc = Utils.getFragmentsCount(this.fragmentsPath);

            let ft = await Utils.getTriplesFromFile(this.fragmentsPath + '/' + fragment.toISOString() + '.trig');
            let fragmentTriples = await Utils.formatTriples('application/trig', ft[1], ft[0]);
            let uri = this._baseUri + '?time=' + fragment.toISOString(); 
            let metaData = await this.createMetadata(uri, index);
            ctx.response.body = this._staticTriples.concat('\n' + fragmentTriples, '\n' + metaData);

            ctx.response.set({
                'Content-Type': 'application/trig'
            });

            if (index < (fc - 1)) {
                // Cache older fragment that won't change over time
                ctx.response.set({ 'Cache-Control': 'public, max-age=31536000, inmutable' });
            } else {
                // Do not cache current fragment as it will get more data
                ctx.response.set({ 'Cache-Control': 'no-cache, no-store, must-revalidate' });
            }
        });
    }

    async storeData(_data, _gat) {
        if (this.byteCounter === 0 || this.byteCounter > this.fragmentMaxSize) {
            // Create new fragment
            this.lastFragment = this.fragmentsPath + '/' + _gat.toISOString() + '.trig';
            this.byteCounter = 0;
        }

        await Utils.appendToFile(this.lastFragment, _data.toString());
        let bytes = Buffer.from(_data.toString()).byteLength;
        this.byteCounter += bytes;
    }

    async createMetadata(subject, index) {
       let quads = [];

        // Add Hydra search template
        quads.push(quad(
            namedNode(subject),
            namedNode('http://www.w3.org/ns/hydra/core#search'),
            namedNode(subject + '#search'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#search'),
            namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            namedNode('http://www.w3.org/ns/hydra/core#IriTemplate'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#search'),
            namedNode('http://www.w3.org/ns/hydra/core#template'),
            literal(this._baseUri + '{?time}'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#search'),
            namedNode('http://www.w3.org/ns/hydra/core#variableRepresentation'),
            namedNode('http://www.w3.org/ns/hydra/core#BasicRepresentation'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#search'),
            namedNode('http://www.w3.org/ns/hydra/core#mapping'),
            namedNode(subject + '#mapping'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#mapping'),
            namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
            namedNode('http://www.w3.org/ns/hydra/core#IriTemplateMapping'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#mapping'),
            namedNode('http://www.w3.org/ns/hydra/core#variable'),
            literal('time'),
            namedNode('#Metadata')
        ));

        quads.push(quad(
            namedNode(subject + '#mapping'),
            namedNode('http://www.w3.org/ns/hydra/core#required'),
            literal(true), //, 'http://www.w3.org/2001/XMLSchema#boolean'),
            namedNode('#Metadata')
        ));

        if (this.license != '') {
            quads.push(quad(
                namedNode(subject),
                namedNode('http://creativecommons.org/ns#license'),
                namedNode(this.license),
                namedNode('#Metadata')
            ));
        }

        if (index > 0) {
            // Adding hydra:previous link
            let fragments = Utils.getAllFragments(this.fragmentsPath);
            let previous = fragments[index - 1].substring(0, fragments[index - 1].indexOf('.trig'));

            quads.push(quad(
                namedNode(subject),
                namedNode('http://www.w3.org/ns/hydra/core#previous'),
                namedNode(this.serverUrl + this.name + '/fragments?time=' + previous),
                namedNode('#Metadata')
            ));
        }
        
        return await Utils.formatTriples('application/trig', quads)
    }

    async fetchStaticTriples(path) {
        return await Utils.getTriplesFromFile(path);
    }

    async formatTriples(format, triples, prefixes) {
        return await Utils.formatTriples(format, triples, prefixes);
    }

    get serverUrl() {
        return this._serverUrl;
    }

    get name() {
        return this._name;
    }

    get websocket() {
        return this._websocket;
    }

    get fragmentsPath() {
        return this._fragmentsPath;
    }

    get fragmentMaxSize() {
        return this._fragmentMaxSize;
    }

    get staticTriples() {
        return this._staticTriples;
    }

    get byteCounter() {
        return this._byteCounter;
    }

    set byteCounter(value) {
        this._byteCounter = value;
    }

    get lastGat() {
        return this._lastGat;
    }

    get license() {
        return this._license;
    }

    set lastGat(gat) {
        this._lastGat = gat;
    }
}

module.exports = RawData;
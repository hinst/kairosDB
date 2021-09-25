import fetch, { RequestInit } from 'node-fetch';

import http from 'http';
import https from 'https';
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const agent = (_parsedURL: URL) => _parsedURL.protocol == 'https:' ? httpsAgent : httpAgent;

type KairosMultiTags = { [key:string]: string[] };
type KairosTags = { [key: string]: string };

export class KairosAggregator {
    name: string;
    align_sampling: boolean;
    sampling: {
        /** Should put a number into this string */
        value: string;
        unit: string;
    }
}

export enum KairosGrouperType {
    tag = 'tag',
}

export class KairosGrouper {
    name: KairosGrouperType;
    tags: string[];
}

export class KairosRequestMetric {
    name: string;
    tags: KairosMultiTags;
    group_by: KairosGrouper[];
    aggregators: KairosAggregator[];
    constructor(metricName: string = null) {
        if (metricName)
            this.name = metricName;
    }
    putTag(key: string, values: string[]) {
        if (this.tags == null)
            this.tags = {};
        this.tags[key] = values;
        return this;
    }
}

export class KairosRequest {
    cache_time? = 0;
    start_absolute? = 0;
    end_absolute? :number;
    time_zone?: string;
    metrics: KairosRequestMetric[];
    addMetric(metric: KairosRequestMetric) {
        if (this.metrics == null)
            this.metrics = [];
        this.metrics.push(metric);
        return this;
    }
}

/** A point in KairosDB.
 * First element of the array is the date.
 * Second element of the array is the value. */
export type KairosItem = [number, number];
export class KairosResult {
    name: string;
    tags: KairosMultiTags;
    values: KairosItem[];
}

export class KairosQueryResponse {
    sample_size: number;
    results: KairosResult[];
}
export class KairosResponse {
    queries: KairosQueryResponse[];
    errors: string[];
}

export class KairosIncoming {
    /** Metric name */
    name: string;
    /** KairosDB: at least one tag is required */
    tags: KairosTags;
    datapoints: KairosItem[];
    constructor(metricName: string = null) {
        if (metricName)
            this.name = metricName;
    }
    putTag(key: string, value: string) {
        if (this.tags == null)
            this.tags = {};
        this.tags[key] = value;
        return this;
    }
    setValues(items: KairosItem[]) {
        this.datapoints = items;
        return this;
    }
}

export class TimeIntervalCountingSettings {
    interval: number;
    deadInterval: number;
}

/** For extra security: use ReadOnlyKairosDB to make sure nothing is erased or changed */
export class ReadOnlyKairosDB {
    url: string = "";
    static allYears = 999_999;
    get apiUrl() {
        return this.url + "/api/v1";
    }

    get queryUrl() {
        return this.apiUrl + "/datapoints/query";
    }

    async getMetricNames(prefix: string = null): Promise<string[]> {
        const prefixUrlPart = prefix != null ? '?prefix=' + encodeURIComponent(prefix) : '';
        const response = await fetch(this.apiUrl + '/metricnames' + prefixUrlPart, {
            agent,
            compress: true,
        });
        const responseObject: any = await response.json();
        return responseObject.results;
    }

    async getMetricTags(metricName: string): Promise<KairosMultiTags> {
        const request = {
            cache_time: 0,
            start_absolute: 0,
            metrics: [{ name: metricName, tags: {}}],
        }
        const response = await fetch(this.queryUrl + "/tags", this.getPostJsonRequest(request));
        const responseObject = await response.json() as KairosResponse;
        const tags = responseObject.queries[0].results[0].tags;
        return tags;
    }

    protected getPostJsonRequest(requestObject: any) {
        const request: RequestInit = {
            agent,
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(requestObject),
            compress: true,
        }
        return request;
    }

    async read(requestObject: KairosRequest): Promise<KairosResponse> {
        const request = this.getPostJsonRequest(requestObject);
        const url = this.apiUrl + "/datapoints/query";
        const response = await fetch(url, request);
        const obj = await response.json() as KairosResponse;
        if (obj.errors)
            throw obj.errors.join('\n');
        return obj;
    }

    createSimpleRequest(metricName: string, tags: KairosMultiTags = null) {
        const metric = new KairosRequestMetric();
        metric.name = metricName;
        if (tags != null)
            metric.tags = tags;
        const request = new KairosRequest();
        request.cache_time = 0;
        request.start_absolute = 1;
        request.metrics = [metric];
        return request;
    }

    async readCount(request: KairosRequest): Promise<number> {
        request.metrics.forEach(m =>
            m.aggregators = [
                {
                    name: "count",
                    "align_sampling": true,
                    "sampling": {"value": '' + ReadOnlyKairosDB.allYears, "unit": "years"}
                }
            ]
        );
        const output = await this.read(request);
        return output.queries[0].sample_size ? output.queries[0].results[0].values[0][1] : 0;
    }

    /** useless function? */
    async readCountLong(metricName: string, tags: KairosMultiTags, settings: TimeIntervalCountingSettings): Promise<number> {
        if (!(settings.interval > 1))
            throw new Error('settings.interval must be greater than 1');
        const request = this.createSimpleRequest(metricName, tags);
        request.metrics[0].aggregators = [
            {
                name: "count",
                "align_sampling": true,
                "sampling": {"value": '' + ReadOnlyKairosDB.allYears, "unit": "years"}
            }
        ];
        let currentTime = new Date().getTime();
        let nonZeroTime = currentTime;
        let totalCount = 0;
        while (0 < currentTime && Math.abs(currentTime - nonZeroTime) <= settings.deadInterval) {
            request.end_absolute = currentTime;
            request.start_absolute = currentTime - settings.interval + 1;
            if (request.start_absolute < 0)
                request.start_absolute = 0;
            const output = await this.read(request);
            const currentCount = output.queries[0].sample_size ? output.queries[0].results[0].values[0][1] : 0;
            if (currentCount > 0)
                nonZeroTime = currentTime;
            totalCount += currentCount;
            currentTime -= settings.interval;
        }
        return totalCount;
    }
}

export class KairosDB extends ReadOnlyKairosDB {
    async deleteMetric(metricName: string) {
        const response = await fetch(this.apiUrl + '/metric/' + encodeURIComponent(metricName), {
            agent,
            method: 'DELETE',
        });
        if (response.status != 204)
            throw new Error(await response.text());
        return response;
    }

    async write(items: KairosIncoming[]) {
        const response = await fetch(this.apiUrl + '/datapoints', this.getPostJsonRequest(items));
        if (response.status != 204)
            throw new Error(await response.text());
    }

    async delete(request: KairosRequest) {
        const response = await fetch(this.apiUrl + '/datapoints/delete', this.getPostJsonRequest(request));
        if (response.status != 204)
            throw new Error(await response.text());
        return response;
    }
}

export function checkKairosItemsEqual(left: KairosItem[], right: KairosItem[]) {
    if (left.length != right.length)
        return false;
    for (let i = 0; i < left.length; ++i) {
        if (left[i][0] !== right[i][0])
            return false;
        if (left[i][1] !== right[i][1])
            return false;
    }
    return true;
}

export enum KairosAggregatorUnits {
    MILLISECONDS = 'MILLISECONDS',
    SECONDS = 'SECONDS',
    MINUTES = 'MINUTES',
    HOURS = 'HOURS',
    DAYS = 'DAYS',
    WEEKS = 'WEEKS',
    MONTHS = 'MONTHS',
    YEARS = 'YEARS',
}

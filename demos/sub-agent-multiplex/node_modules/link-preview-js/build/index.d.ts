/// <reference types="cheerio" />
interface ILinkPreviewResponse {
    url: string;
    title: string;
    siteName: string | undefined;
    author: string | undefined;
    description: string | undefined;
    mediaType: string;
    contentType: string | undefined;
    images: string[];
    videos: IVideoType[];
    favicons: string[];
}
interface IVideoType {
    url: string | undefined;
    secureUrl: string | null | undefined;
    type: string | null | undefined;
    width: string | undefined;
    height: string | undefined;
}
interface ILinkPreviewOptions {
    headers?: Record<string, string>;
    imagesPropertyType?: string;
    proxyUrl?: string;
    timeout?: number;
    followRedirects?: `follow` | `error` | `manual`;
    resolveDNSHost?: (url: string) => Promise<string>;
    handleRedirects?: (baseURL: string, forwardedURL: string) => boolean;
    onResponse?: (response: ILinkPreviewResponse, doc: cheerio.Root, url?: URL) => ILinkPreviewResponse;
}
interface IPreFetchedResource {
    headers: Record<string, string>;
    status?: number;
    imagesPropertyType?: string;
    proxyUrl?: string;
    url: string;
    data: string;
}
/**
 * Parses the text, extracts the first link it finds and does a HTTP request
 * to fetch the website content, afterwards it tries to parse the internal HTML
 * and extract the information via meta tags
 * @param text string, text to be parsed
 * @param options ILinkPreviewOptions
 */
export declare function getLinkPreview(text: string, options?: ILinkPreviewOptions): Promise<ILinkPreviewResponse | {
    charset: string | null;
    url: string;
    mediaType: string;
    contentType: string;
    favicons: string[];
} | {
    charset: string | null;
    url: string;
    title: string;
    siteName: string | undefined;
    author: string | undefined;
    description: string | undefined;
    mediaType: string;
    contentType: string | undefined;
    images: string[];
    videos: IVideoType[];
    favicons: string[];
}>;
/**
 * Skip the library fetching the website for you, instead pass a response object
 * from whatever source you get and use the internal parsing of the HTML to return
 * the necessary information
 * @param response Preview Response
 * @param options IPreviewLinkOptions
 */
export declare function getPreviewFromContent(response: IPreFetchedResource, options?: ILinkPreviewOptions): Promise<ILinkPreviewResponse | {
    charset: string | null;
    url: string;
    mediaType: string;
    contentType: string;
    favicons: string[];
} | {
    charset: string | null;
    url: string;
    title: string;
    siteName: string | undefined;
    author: string | undefined;
    description: string | undefined;
    mediaType: string;
    contentType: string | undefined;
    images: string[];
    videos: IVideoType[];
    favicons: string[];
}>;
export {};

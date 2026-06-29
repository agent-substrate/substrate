declare function tls(
  ikm: Buffer | string,
  length: number,
  option?: tls.Options
): Buffer;

declare namespace tls {
  interface Options {
    salt?: Buffer | string;
    label?: string;
    context?: Buffer | string;
    hash?: string;
  }
  export function info(
    length: number,
    label: string,
    context?: Buffer | string,
  ): Buffer;
  export function expand_label(
    hash: string,
    hash_len: number,
    prk: Buffer | string,
    length: number,
    label: string,
    context?: Buffer | string,
  ): Buffer;
}

export = tls;

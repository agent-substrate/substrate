import { RedisCommandArguments } from '.';
export declare enum ClientKillFilters {
    ADDRESS = "ADDR",
    LOCAL_ADDRESS = "LADDR",
    ID = "ID",
    TYPE = "TYPE",
    USER = "USER",
    SKIP_ME = "SKIPME",
    MAXAGE = "MAXAGE"
}
interface KillFilter<T extends ClientKillFilters> {
    filter: T;
}
interface KillAddress extends KillFilter<ClientKillFilters.ADDRESS> {
    address: `${string}:${number}`;
}
interface KillLocalAddress extends KillFilter<ClientKillFilters.LOCAL_ADDRESS> {
    localAddress: `${string}:${number}`;
}
interface KillId extends KillFilter<ClientKillFilters.ID> {
    id: number | `${number}`;
}
interface KillType extends KillFilter<ClientKillFilters.TYPE> {
    type: 'normal' | 'master' | 'replica' | 'pubsub';
}
interface KillUser extends KillFilter<ClientKillFilters.USER> {
    username: string;
}
type KillSkipMe = ClientKillFilters.SKIP_ME | (KillFilter<ClientKillFilters.SKIP_ME> & {
    skipMe: boolean;
});
interface KillMaxAge extends KillFilter<ClientKillFilters.MAXAGE> {
    maxAge: number;
}
type KillFilters = KillAddress | KillLocalAddress | KillId | KillType | KillUser | KillSkipMe | KillMaxAge;
export declare function transformArguments(filters: KillFilters | Array<KillFilters>): RedisCommandArguments;
export declare function transformReply(): number;
export {};

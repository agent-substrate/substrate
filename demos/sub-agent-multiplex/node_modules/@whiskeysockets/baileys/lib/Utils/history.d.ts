import { AxiosRequestConfig } from 'axios';
import { proto } from '../../WAProto';
import { Chat, Contact } from '../Types';
export declare const downloadHistory: (msg: proto.Message.IHistorySyncNotification, options: AxiosRequestConfig<any>) => Promise<proto.HistorySync>;
export declare const processHistoryMessage: (item: proto.IHistorySync) => {
    chats: Chat[];
    contacts: Contact[];
    messages: proto.IWebMessageInfo[];
};
export declare const downloadAndProcessHistorySyncNotification: (msg: proto.Message.IHistorySyncNotification, options: AxiosRequestConfig<any>) => Promise<{
    chats: Chat[];
    contacts: Contact[];
    messages: proto.IWebMessageInfo[];
}>;
export declare const getHistoryMsg: (message: proto.IMessage) => proto.Message.IHistorySyncNotification | null | undefined;

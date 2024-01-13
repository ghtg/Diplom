'use strict';

require('isomorphic-fetch');
var url = require('url');
var events = require('events');
var WebSocket = require('ws');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

var WebSocket__default = /*#__PURE__*/_interopDefaultLegacy(WebSocket);

/**
 * @hidden
 */
class Streaming extends events.EventEmitter {
    /**
     *
     * @param apiURL REST api url см [документацию](https://tinkoffcreditsystems.github.io/invest-openapi/env/)
     * @param socketURL Streaming api url см [документацию](https://tinkoffcreditsystems.github.io/invest-openapi/env/)
     * @param secretToken токен доступа см [получение токена доступа](https://tinkoffcreditsystems.github.io/invest-openapi/auth/)
     *
     */
    constructor({ url, secretToken }) {
        super();
        this._ws = null;
        this._wsQueue = [];
        this._subscribeMessages = [];
        /**
         * Обработчик открытия соединения
         */
        this.handleSocketOpen = (e) => {
            // Восстанавливаем подписки
            if (this._ws && this._subscribeMessages) {
                this._wsQueue.length = 0;
                this._subscribeMessages.forEach((msg) => {
                    this.enqueue(msg);
                });
            }
            this.emit('socket-open', e);
            this.dispatchWsQueue();
            this.socketPingLoop();
        };
        /**
         * Зацикленная отправка пингов на сервер
         */
        this.socketPingLoop = () => {
            if (this._ws) {
                this._ws.ping('ping');
                this._wsPingTimeout = setTimeout(this.socketPingLoop, 15000);
            }
        };
        /**
         * Обработчик закрытия соединения
         */
        this.handleSocketClose = (e) => {
            this.emit('socket-close', e);
            this.handleSocketError();
        };
        /**
         * Обработчик ошибок и переподключение при необходимости
         */
        this.handleSocketError = (e) => {
            var _a;
            clearTimeout(this._wsPingTimeout);
            this.emit('socket-error', e);
            if (!this._ws) {
                return;
            }
            const isClosed = [2 /* CLOSING */, 3 /* CLOSED */].includes((_a = this._ws) === null || _a === void 0 ? void 0 : _a.readyState);
            this._ws.off('open', this.handleSocketOpen);
            this._ws.off('message', this.handleSocketMessage);
            this._ws.off('close', this.handleSocketClose);
            this._ws.off('error', this.handleSocketError);
            if (isClosed) {
                this._ws.terminate();
                this._ws = null;
                if (this._subscribeMessages.length) {
                    // не делаем реконнект если нет активных подписок
                    this.connect();
                }
            }
        };
        /**
         * Обработчик входящих сообщений
         */
        this.handleSocketMessage = (m) => {
            const { event: type, payload, time: serverTime } = JSON.parse(m);
            const otherFields = { serverTime };
            if (type === 'error') {
                this.emit('streaming-error', payload, otherFields);
            }
            else {
                this.emit(this.getEventName(type, payload), payload, otherFields);
            }
        };
        this.socketURL = url;
        this.secretToken = secretToken;
        this.authHeaders = {
            Authorization: 'Bearer ' + this.secretToken,
            'Content-Type': 'application/json',
        };
    }
    /**
     * Соединяемся с сокетом
     */
    connect() {
        if (this._ws && [1 /* OPEN */, 0 /* CONNECTING */].includes(this._ws.readyState)) {
            return;
        }
        this._ws = new WebSocket__default['default'](this.socketURL, {
            handshakeTimeout: 4000,
            perMessageDeflate: false,
            headers: this.authHeaders,
        });
        this._ws.on('open', this.handleSocketOpen);
        this._ws.on('message', this.handleSocketMessage);
        this._ws.on('close', this.handleSocketClose);
        this._ws.on('error', this.handleSocketError);
    }
    /**
     * Получение имени ивента
     */
    getEventName(type, params) {
        if (type === 'orderbook') {
            return `${type}-${params.figi}-${params.depth}`;
        }
        if (type === 'candle') {
            return `${type}-${params.figi}-${params.interval}`;
        }
        if (type === 'instrument_info') {
            return `${type}-${params.figi}`;
        }
        if (type === 'error') {
            return 'streaming-error';
        }
        throw new Error(`Unknown type: ${type}`);
    }
    /**
     * Поставить сообщение в очередь на отправку в сокет
     */
    enqueue(command) {
        this._wsQueue.push(command);
        this.dispatchWsQueue();
    }
    /**
     * Разбор очереди сообщений на отправку в сокет
     */
    dispatchWsQueue() {
        var _a;
        if (((_a = this._ws) === null || _a === void 0 ? void 0 : _a.readyState) === 1 /* OPEN */) {
            const cb = () => this._wsQueue.length && this.dispatchWsQueue();
            this._ws.send(JSON.stringify(this._wsQueue.shift()), cb);
        }
    }
    /**
     * Подписка на различные каналы в сокете
     */
    subscribeToSocket({ type, ...params }, cb) {
        if (!this._ws) {
            this.connect();
        }
        let eventName = this.getEventName(type, params);
        const message = { event: `${type}:subscribe`, ...params };
        if (!this.listenerCount(eventName)) {
            this.enqueue(message);
            this._subscribeMessages.push(message);
        }
        const handler = (x) => setImmediate(() => cb(x));
        this.on(eventName, handler);
        return () => {
            var _a;
            this.off(eventName, handler);
            if (!this.listenerCount(eventName)) {
                this.enqueue({ event: `${type}:unsubscribe`, ...params });
                const index = this._subscribeMessages.findIndex((msg) => msg === message);
                if (index !== -1) {
                    this._subscribeMessages.splice(index, 1);
                }
                if (!this._subscribeMessages.length) {
                    (_a = this._ws) === null || _a === void 0 ? void 0 : _a.close();
                }
            }
        };
    }
    orderbook({ figi, depth = 3 }, cb = console.log) {
        return this.subscribeToSocket({ type: 'orderbook', figi, depth }, cb);
    }
    /**
     * Метод для подписки на данные по свечному графику инструмента
     * @example см. метод [[orderbook]]
     * @param figi идентификатор инструмента
     * @param interval интервал для свечи
     * @param cb функция для обработки новых данных по свечи
     * @return функция для отмены подписки
     */
    candle({ figi, interval = '1min' }, cb = console.log) {
        return this.subscribeToSocket({ type: 'candle', figi, interval }, cb);
    }
    /**
     * Метод для подписки на данные по инструменту
     * @example см. метод [[orderbook]]
     * @param figi идентификатор инструмента
     * @param cb функция для обработки новых данных по инструменту
     * @return функция для отмены подписки
     */
    instrumentInfo({ figi }, cb = console.log) {
        return this.subscribeToSocket({ type: 'instrument_info', figi }, cb);
    }
    /**
     * Метод для обработки ошибки от сервиса стриминга
     * @example см. метод [[onStreamingError]]
     * @param cb
     * @return функция для отмены подписки
     */
    onStreamingError(cb) {
        this.on('streaming-error', cb);
        return () => {
            this.off('streaming-error', cb);
        };
    }
}

const omitUndef = (x) => JSON.parse(JSON.stringify(x));
function getQueryString(params) {
    // must be a number https://github.com/microsoft/TypeScript/issues/32951
    const searchParams = new url.URLSearchParams(omitUndef(params)).toString();
    return searchParams.length ? `?${searchParams}` : '';
}
class OpenAPI {
    /**
     *
     * @param apiURL REST api url см [документацию](https://tinkoffcreditsystems.github.io/invest-openapi/env/)
     * @param socketURL Streaming api url см [документацию](https://tinkoffcreditsystems.github.io/invest-openapi/env/)
     * @param secretToken токен доступа см [получение токена доступа](https://tinkoffcreditsystems.github.io/invest-openapi/auth/)
     * @param brokerAccountId номер счета (по умолчанию - Тинькофф)
     */
    constructor({ apiURL, socketURL, secretToken, brokerAccountId }) {
        this._sandboxCreated = false;
        this._currentBrokerAccountId = undefined;
        this._streaming = new Streaming({ url: socketURL, secretToken });
        this._currentBrokerAccountId = brokerAccountId;
        this.apiURL = apiURL;
        this.secretToken = secretToken;
        this.authHeaders = {
            Authorization: 'Bearer ' + this.secretToken,
            'Content-Type': 'application/json',
        };
    }
    /**
     * Запрос к REST
     */
    async makeRequest(url, { method = 'get', query, body } = {}) {
        let requestParams = { method, headers: this.authHeaders };
        let requestUrl = this.apiURL + url + getQueryString(query || {});
        if (method === 'post') {
            requestParams.body = JSON.stringify(body);
        }
        const res = await fetch(requestUrl, requestParams);
        // XXX для консистентности ошибок от API
        if (res.status === 401) {
            throw {
                status: 'Error',
                message: 'Unauthorized! Try to use valid token. https://tinkoffcreditsystems.github.io/invest-openapi/auth/',
            };
        }
        if (res.status === 429) {
            throw {
                status: 'Error',
                message: 'Too Many Requests!',
            };
        }
        if (!res.ok) {
            throw await res.json();
        }
        const data = await res.json();
        return data.payload;
    }
    /**
     * Регистрация песочницы
     */
    sandboxRegister() {
        if (!this._sandboxCreated) {
            this.makeRequest('/sandbox/register', { method: 'post' });
            this._sandboxCreated = true;
        }
    }
    /**
     * Метод возвращает текущий номер счета (*undefined* - значение по умолчанию для счета Тинькофф).
     */
    getCurrentAccountId() {
        return this._currentBrokerAccountId;
    }
    /**
     * Метод для сохранения номера счета по умолчанию.
     * @param brokerAccountId - Номер счета. Для счета Тинькофф можно также передать значение *undefined*.
     */
    setCurrentAccountId(brokerAccountId) {
        this._currentBrokerAccountId = brokerAccountId;
    }
    /**
     * Метод для очистки песочницы
     */
    async sandboxClear() {
        await this.sandboxRegister();
        return this.makeRequest('/sandbox/clear', {
            method: 'post',
            query: { brokerAccountId: this._currentBrokerAccountId },
        });
    }
    /**
     * Метод для задания баланса по бумагам
     * @param params см. описание типа
     */
    async setPositionBalance(params) {
        await this.sandboxRegister();
        return this.makeRequest('/sandbox/positions/balance', {
            method: 'post',
            query: { brokerAccountId: this._currentBrokerAccountId },
            body: params,
        });
    }
    /**
     * Метод для задания баланса по валютам
     * @param params см. описание типа
     */
    async setCurrenciesBalance(params) {
        await this.sandboxRegister();
        return this.makeRequest('/sandbox/currencies/balance', {
            method: 'post',
            query: { brokerAccountId: this._currentBrokerAccountId },
            body: params,
        });
    }
    /**
     * Метод для получение портфеля цб
     */
    portfolio() {
        return this.makeRequest('/portfolio', {
            query: { brokerAccountId: this._currentBrokerAccountId },
        });
    }
    /**
     * Метод для получения валютных активов клиента
     */
    portfolioCurrencies() {
        return this.makeRequest('/portfolio/currencies', {
            query: { brokerAccountId: this._currentBrokerAccountId },
        });
    }
    /**
     * Метод для получение данных по инструменту в портфеле
     * @param params см. описание типа
     */
    instrumentPortfolio(params) {
        return this.portfolio().then((x) => {
            return (x.positions.find((position) => {
                if ('figi' in params) {
                    return position.figi === params.figi;
                }
                if ('ticker' in params) {
                    return position.ticker === params.ticker;
                }
            }) || null);
        });
    }
    /**
     * Метод для выставления заявки
     * @param figi идентификатор инструмента
     * @param lots количество лотов для заявки
     * @param operation тип заявки
     * @param price цена лимитной заявки
     */
    limitOrder({ figi, lots, operation, price, }) {
        return this.makeRequest('/orders/limit-order', {
            method: 'post',
            query: {
                figi,
                brokerAccountId: this._currentBrokerAccountId,
            },
            body: {
                lots,
                operation,
                price,
            },
        });
    }
    /**
     * Метод для выставления заявки
     * @param figi идентификатор инструмента
     * @param lots количество лотов для заявки
     * @param operation тип заявки
     * @param price цена лимитной заявки
     */
    marketOrder({ figi, lots, operation }) {
        return this.makeRequest('/orders/market-order', {
            method: 'post',
            query: {
                figi,
                brokerAccountId: this._currentBrokerAccountId,
            },
            body: {
                lots,
                operation,
            },
        });
    }
    //todo протестить
    /**
     * Метод для отмены активных заявок
     * @param orderId идентифткатор заявки
     */
    cancelOrder({ orderId }) {
        return this.makeRequest(`/orders/cancel`, {
            method: 'post',
            query: {
                orderId,
                brokerAccountId: this._currentBrokerAccountId,
            },
        });
    }
    /**
     * Метод для получения всех активных заявок
     */
    orders() {
        return this.makeRequest('/orders', {
            query: { brokerAccountId: this._currentBrokerAccountId },
        });
    }
    /**
     * Метод для получения всех доступных валютных инструментов
     */
    currencies() {
        return this.makeRequest('/market/currencies');
    }
    /**
     * Метод для получения всех доступных валютных ETF
     */
    etfs() {
        return this.makeRequest('/market/etfs');
    }
    /**
     * Метод для получения всех доступных облигаций
     */
    bonds() {
        return this.makeRequest('/market/bonds');
    }
    /**
     * Метод для получения всех доступных акций
     */
    stocks() {
        return this.makeRequest('/market/stocks');
    }
    /**
     * Метод для получения операций по цб по инструменту
     * @param from Начало временного промежутка в формате ISO 8601
     * @param to Конец временного промежутка в формате ISO 8601
     * @param figi Figi-идентификатор инструмента
     */
    operations({ from, to, figi }) {
        return this.makeRequest('/operations', {
            query: {
                from,
                to,
                figi,
                brokerAccountId: this._currentBrokerAccountId,
            },
        });
    }
    /**
     * Метод для получения исторических свечей по FIGI
     * @param from Начало временного промежутка в формате ISO 8601
     * @param to Конец временного промежутка в формате ISO 8601
     * @param figi Figi-идентификатор инструмента
     * @param interval интервал для свечи
     */
    candlesGet({ from, to, figi, interval = '1min', }) {
        return this.makeRequest('/market/candles', {
            query: { from, to, figi, interval },
        });
    }
    /**
     * Метод для получение стакана
     * @param figi Figi-идентификатор инструмента
     * @param depth
     */
    orderbookGet({ figi, depth = 3 }) {
        return this.makeRequest('/market/orderbook', {
            query: { figi, depth },
        });
    }
    /**
     * Метод для поиска инструментов по figi или ticker
     * @param params { figi или ticker }
     */
    search(params) {
        if ('figi' in params) {
            return this.makeRequest('/market/search/by-figi', {
                query: { figi: params.figi },
            }).then((x) => (x ? { total: 1, instruments: [x] } : { total: 0, instruments: [] }));
        }
        if ('ticker' in params) {
            return this.makeRequest('/market/search/by-ticker', {
                query: { ticker: params.ticker },
            });
        }
        throw new Error('should specify figi or ticker');
    }
    /**
     * Метод для поиска инструмента по figi или ticker
     * @param params { figi или ticker }
     */
    searchOne(params) {
        return this.search(params).then((x) => x.instruments[0] || null);
    }
    /**
     * Метод для подписки на данные по стакану инструмента
     * @example
     * ```typescript
     * const { figi } = await api.searchOne({ ticker: 'AAPL' });
     * const unsubFromAAPL = api.orderbook({ figi }, (ob) => { console.log(ob.bids) });
     * // ... подписка больше не нужна
     * unsubFromAAPL();
     * ```
     * @param figi идентификатор инструмента
     * @param depth
     * @param cb функция для обработки новых данных по стакану
     * @return функция для отмены подписки
     */
    orderbook({ figi, depth = 3 }, cb = console.log) {
        return this._streaming.orderbook({ figi, depth }, cb);
    }
    /**
     * Метод для подписки на данные по свечному графику инструмента
     * @example см. метод [[orderbook]]
     * @param figi идентификатор инструмента
     * @param interval интервал для свечи
     * @param cb функция для обработки новых данных по свечи
     * @return функция для отмены подписки
     */
    candle({ figi, interval = '1min' }, cb = console.log) {
        return this._streaming.candle({ figi, interval }, cb);
    }
    /**
     * Метод для подписки на данные по инструменту
     * @example см. метод [[orderbook]]
     * @param figi идентификатор инструмента
     * @param cb функция для обработки новых данных по инструменту
     * @return функция для отмены подписки
     */
    instrumentInfo({ figi }, cb = console.log) {
        return this._streaming.instrumentInfo({ figi }, cb);
    }
    /**
     * Метод для обработки сообщений об ошибки от стриминга
     * @example
     * ```typescript
     * api.onStreamingError(({ error }) => { console.log(error) });
     * api.instrumentInfo({ figi: 'NOOOOOOO' }, (ob) => { console.log(ob.bids) });
     * // logs:  Subscription instrument_info:subscribe. FIGI NOOOOOOO not found
     * ```
     * @param cb функция для обработки всех ошибок от стриминга
     * @return функция для отмены подписки
     */
    onStreamingError(cb) {
        return this._streaming.onStreamingError(cb);
    }
    /**
     * Метод для получения брокерских счетов клиента
     */
    accounts() {
        return this.makeRequest('/user/accounts');
    }
}

module.exports = OpenAPI;

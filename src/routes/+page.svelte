<script lang="ts">
    import {
        ArrayQueue,
        ExponentialBackoff,
        Websocket,
        WebsocketBuilder,
        WebsocketEvent,
    } from "websocket-ts";

    import Chart from 'chart.js/auto';
    import 'chartjs-adapter-date-fns';

    import { decode, decodeAsync, decodeMulti as mpDecode } from "@msgpack/msgpack";

    const FIXED_POINT_MULT = 2 ** (31)

    interface AssetIdPair {
        primary: number,
        secondary: number,
    }

    interface Asset {
        id: number,
        name: string,
        symbol: string, 
    }

    interface PriceLevelAggregate {
        price: number,
        volume: number,
    }

    interface MarketL1 {
        pair: AssetIdPair,
        orderbook: OrderbookL1,
    }

    interface OrderbookL1 {
        best_bid: PriceLevelAggregate | null,
        best_ask: PriceLevelAggregate | null,
    }

    interface ExchangeMetrics {
        p50: number,
        p90: number,
        p999: number,
    }

    interface ExchangeState {
        l1s: MarketL1[],
        metrics: ExchangeMetrics,
    }

    let DEFAULT_EXCHANGE_STATE: ExchangeState = {
        l1s: [],
        metrics: {
            p50: 0,
            p90: 0,
            p999: 0,
        }
    }

    let exchangeState: ExchangeState = $state(DEFAULT_EXCHANGE_STATE);
    let exchangeAssets: Asset[] = $state([]);
    let receivedAssetsMessage = false;

    async function decodeFromBlob(blob: Blob): Promise<unknown> {
        if (blob.stream) {
            // Blob#stream(): ReadableStream<Uint8Array> (recommended)
            return await decodeAsync(blob.stream());
        } else {
            // Blob#arrayBuffer(): Promise<ArrayBuffer> (if stream() is not available)
            return decode(await blob.arrayBuffer());
        }
    }

    // Initialize WebSocket with buffering and 1s reconnection delay
    const ws = new WebsocketBuilder("ws://127.0.0.1:5556")
        .withBuffer(new ArrayQueue()) // buffer messages when disconnected
        .withBackoff(new ExponentialBackoff(1000, 6)) // retry every 1s, max of 64s
        .build();
    let exchangeConnected: boolean = $state(false);

    function parse_mp_price(n: number) {
        return n / FIXED_POINT_MULT
    }

    async function updateAssets(messageData: any) {
        let assets = await decodeFromBlob(messageData);
        exchangeAssets = [];
        for (const asset_raw of assets) {
            let asset: Asset = {
                id: asset_raw[0],
                name: asset_raw[1],
                symbol: asset_raw[2]
            }
            exchangeAssets.push(asset);
        }
    }

    function getAssetSymbol(id: number) {
        let asset = exchangeAssets.find((a) => a.id == id);
        if (asset !== undefined) {
            return asset.symbol;
        } else {
            return "!";
        }
    }

    // Function to output & echo received messages
    async function updateExchangeState(messageData: any) {
        let exchangeStateDecoded = await decodeFromBlob(messageData);
        let l1sDecoded = exchangeStateDecoded[0];
        let metricsDecoded = exchangeStateDecoded[1];
        
        // Update l1s
        exchangeState.l1s = []
        for (const l1_info of l1sDecoded) {
            let pair: AssetIdPair = {
                primary: l1_info[0][0],
                secondary: l1_info[0][1]
            };

            // Check for null values in best_bid / best_ask
            let best_bid = l1_info[1][0];
            let best_ask = l1_info[1][1];
            if (best_bid != null) {
                best_bid = { price: parse_mp_price(l1_info[1][0][0]), volume: l1_info[1][0][1]};
            }
            if (best_ask != null) {
                best_ask = { price: parse_mp_price(l1_info[1][1][0]), volume: l1_info[1][1][1]};
            }
            let orderbook: OrderbookL1 = {
                best_bid, 
                best_ask,
            }

            let marketL1: MarketL1 = {
                pair,
                orderbook
            }
            exchangeState.l1s.push(marketL1)
        }

        // Update exchange metrics
        exchangeState.metrics.p50  = metricsDecoded[0];
        exchangeState.metrics.p90  = metricsDecoded[1];
        exchangeState.metrics.p999 = metricsDecoded[2];
        p50s.push(exchangeState.metrics.p50);
        p90s.push(exchangeState.metrics.p90);
        p999s.push(exchangeState.metrics.p999);
        latency_arrival_times.push(Date.now());
    };

    async function handleMessage(i: Websocket, ev: MessageEvent) {
        if (!receivedAssetsMessage) {
            receivedAssetsMessage = true;
            await updateAssets(ev.data);
        } else{
            updateExchangeState(ev.data);
        }
    }

    let p50s = $state([]);
    let p90s = $state([]);
    let p999s = $state([]);
    let latency_arrival_times = $state([]);

    let latency_chart_canvas: HTMLCanvasElement;

    $effect(() => {
        let chart = new Chart(
            latency_chart_canvas,
            {
                type: 'line',
                data: {
                    labels: latency_arrival_times,
                    datasets: [
                        { label: 'p50', data: [...p50s] },
                        { label: 'p90', data: [...p90s] },
                        { label: 'p99.9', data: [...p999s] },
                    ]
                },
                options: {
                    scales: { 
                        x: { type: 'time', ticks: { maxTicksLimit: 10 } },
                        y: { type: 'logarithmic', ticks: { maxTicksLimit: 12 } },
                    },
                    animation: false,
                }
            }
        );
        return () => chart.destroy();
    });

    function onConnOpen() {
        console.log("Opened server connection!"); 
        receivedAssetsMessage = false;
        exchangeConnected = true;
    }

    function onConnClose() {
        console.log("Closed server connection.")
        exchangeConnected = false;
    }

    // Add event listeners
    ws.addEventListener(WebsocketEvent.open, onConnOpen);
    ws.addEventListener(WebsocketEvent.close, onConnClose);
    ws.addEventListener(WebsocketEvent.message, handleMessage);

</script>

<div class="w-full h-screen flex flex-col bg-mist-900 text-white">
    <header class="flex justify-around items-center bg-mist-950 p-4 mb-4">
        <h1 class="text-3xl">
            Live Exchange View
        </h1>
        <div class="flex w-1/2 justify-center gap-2">
            <h2 class="font-bold">Order Latency (ms)</h2>
            <div class="flex gap-1">
                <h3>p50: </h3> <p>{exchangeState.metrics.p50 / 1000}</p>
            </div>
            <div class="flex gap-1">
                <h3>p90: </h3> <p>{exchangeState.metrics.p90 / 1000}</p>
            </div>
            <div class="flex gap-1">
                <h3>p99.9: </h3> <p>{exchangeState.metrics.p999 / 1000}</p>
            </div>
        </div>
        <div class="flex items-center justify-between gap-2 font-bold">
            {#if exchangeConnected == true}
                <div class="bg-green-500 border border-gray-300 rounded-3xl w-3 h-3"></div> Connected
            {:else}
                <div class="bg-red-500 border border-gray-300 rounded-3xl w-3 h-3"></div> Disconnected
            {/if}
        </div>
    </header>
    <div class="flex flex-col justify-around items-center">
    <div class="min-w-[500px] w-[50vw] flex flex-col border-2 rounded-lg p-2 bg-[#0e181e]">
        <div class="flex flex-row w-full">
            <div class="w-1/5">
                Symbol
            </div>
            <div class="flex w-4/5" id="bid-ask">
                <div class="w-1/2 flex justify-end pr-2" id="bid">
                    <div class="w-1/2 flex justify-end pr-2">
                    <p>
                        volume
                    </p>
                    </div>
                    <div class="w-1/2 flex justify-end">
                    <p>
                        Bid
                    </p>
                    </div>
                </div>
                <div class="w-1/2 flex justify-start pl-2" id="bid">
                    <div class="w-1/2 flex justify-start">
                    <p>
                        Ask
                    </p>
                    </div>
                    <div class="w-1/2 flex justify-start pl-2">
                    <p>
                        volume
                    </p>
                    </div>
                </div>

            </div>
        </div>
        {#each exchangeState.l1s as marketL1 (`${marketL1.pair.primary},${marketL1.pair.secondary}`) }
            <div class="flex w-full text-xl border-t-2">
                <div class="w-1/5 border-r-2">
                    {getAssetSymbol(marketL1.pair.primary)}/{getAssetSymbol(marketL1.pair.secondary)}
                </div>
                <div class="flex w-4/5" id="market-bid-ask">
                    <div class="w-1/2 flex pr-2 border-r-2" id="market-bid">
                        <div class="w-1/2 flex justify-end pr-2 border-r-2">
                        <p class="text-green-400">
                            {#if marketL1.orderbook.best_bid != null}
                            {marketL1.orderbook.best_bid.volume}
                            {:else}
                            -
                            {/if}
                        </p>
                        </div>
                        <div class="w-1/2 flex justify-end">
                        <p class="font-bold text-green-400">
                        {#if marketL1.orderbook.best_bid != null}
                        {marketL1.orderbook.best_bid.price.toFixed(3)}
                        {:else}
                        -
                        {/if}
                        </p>
                        </div>
                    </div>
                    <div class="w-1/2 flex pl-2 border-l-2" id="market-ask">
                        <div class="w-1/2 justify-start">
                        <p class="font-bold text-red-400">
                            {#if marketL1.orderbook.best_ask != null}
                            {marketL1.orderbook.best_ask.price.toFixed(3)}
                            {:else}
                            -
                            {/if}
                        </p>
                        </div>
                        <div class="w-1/2 justify-start border-l-2 pl-2">
                        <p class="text-red-400 ">
                            {#if marketL1.orderbook.best_ask != null}
                            {marketL1.orderbook.best_ask.volume}
                            {:else}
                            -
                            {/if}
                        </p>
                        </div>
                    </div>
                </div>
            </div>
        {/each}
    </div>
    </div>
    <canvas bind:this={latency_chart_canvas}></canvas>
</div>



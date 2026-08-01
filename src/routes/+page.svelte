<svelte:head>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&display=swap');
    </style>
</svelte:head>
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
  import { Table, TableBody, TableBodyCell, TableBodyRow, TableHead, TableHeadCell, Checkbox, TableSearch } from "flowbite-svelte";

    import { decode, decodeAsync, decodeMulti as mpDecode } from "@msgpack/msgpack";
    import { asset } from "$app/paths";

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
        timestamp: number,
        p50: number,
        p90: number,
        p999: number,
    }

    interface ExchangeState {
        l1s: MarketL1[],
        metrics: ExchangeMetrics,
    }

    interface Transaction {
        pair: AssetIdPair,
        price: number,
        volume: number,
        taker_side: number,
        maker: number,
        taker: number,
        timestamp: Date,
    }

    let DEFAULT_EXCHANGE_STATE: ExchangeState = {
        l1s: [],
        metrics: {
            timestamp: 0,
            p50: 0,
            p90: 0,
            p999: 0,
        }
    }

    let exchangeState: ExchangeState = $state(DEFAULT_EXCHANGE_STATE);
    let exchangeAssets: Asset[] = $state([]);
    let last100Transactions: Transaction[] = $state([]);
    let tps100: number = $derived.by(() => {
        let first = last100Transactions[0];
        let last = last100Transactions[last100Transactions.length - 1];
        if (first != undefined && last != undefined) {
            let first_as_ms = first.timestamp.getTime();
            let last_as_ms = last.timestamp.getTime();
            return 1000 * last100Transactions.length / (first_as_ms - last_as_ms);
        } else {
            return 0;
        }
    }
    );
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

    function getMarketSymbolic(assetIdPair: AssetIdPair) {
        let primary_str = getAssetSymbol(assetIdPair.primary);
        let secondary_str = getAssetSymbol(assetIdPair.secondary);
        return primary_str + "/" + secondary_str
    }

    // Function to output & echo received messages
    async function updateExchangeState(messageData: any) {
        let exchangeStateDecoded = await decodeFromBlob(messageData);
        let l1sDecoded = exchangeStateDecoded[0];
        let metricsDecoded = exchangeStateDecoded[1];
        let last_100_tx = exchangeStateDecoded[2];
        
        // Update l1s

        exchangeState.l1s = []
        for (const l1Decoded of l1sDecoded) {
            let pair: AssetIdPair = {
                primary: l1Decoded[0][0],
                secondary: l1Decoded[0][1]
            };

            // Check for null values in best_bid / best_ask
            let best_bid = l1Decoded[1][0];
            let best_ask = l1Decoded[1][1];
            if (best_bid != null) {
                best_bid = { price: parse_mp_price(l1Decoded[1][0][0]), volume: l1Decoded[1][0][1]};
            }
            if (best_ask != null) {
                best_ask = { price: parse_mp_price(l1Decoded[1][1][0]), volume: l1Decoded[1][1][1]};
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

        // Ensure that the timestamp is larger than the last,
        // so that we're not registering the same metrics twice
        if (metricsDecoded[0] > exchangeState.metrics.timestamp) {
            exchangeState.metrics.timestamp = metricsDecoded[0];
            exchangeState.metrics.p50  = metricsDecoded[1] / 1000;
            exchangeState.metrics.p90  = metricsDecoded[2] / 1000;
            exchangeState.metrics.p999 = metricsDecoded[3] / 1000;
            p50s.push(exchangeState.metrics.p50);
            p90s.push(exchangeState.metrics.p90);
            p999s.push(exchangeState.metrics.p999);
            latency_arrival_times.push(exchangeState.metrics.timestamp);
    
            // Only keep last minute
            p50s = p50s.slice(-30);
            p90s = p90s.slice(-30);
            p999s = p999s.slice(-30);
            latency_arrival_times = latency_arrival_times.slice(-30);
        }

        // Update recent transactions
        last100Transactions = [];
        for (const tx_decoded of last_100_tx) {
            let transaction: Transaction = {
                pair: {
                    primary: tx_decoded[0][0],
                    secondary: tx_decoded[0][1],
                },
                price: parse_mp_price(tx_decoded[1][0]),
                volume: tx_decoded[2],
                taker_side: tx_decoded[3],
                maker: tx_decoded[4],
                taker: tx_decoded[5],
                timestamp: new Date(tx_decoded[6]),
            };
            // Unshift to reverse order of transactions (most recent transaction
            // comes first)
            last100Transactions.unshift(transaction);
        }

    };

    async function handleMessage(i: Websocket, ev: MessageEvent) {
        if (!receivedAssetsMessage) {
            receivedAssetsMessage = true;
            await updateAssets(ev.data);
        } else{
            await updateExchangeState(ev.data);
        }
    }

    let p50s = $state([]);
    let p90s = $state([]);
    let p999s = $state([]);
    let latency_arrival_times = $state([]);

    let latency_chart_canvas: HTMLCanvasElement;
    let chart: Chart | undefined;

    $effect(() => {
        chart = new Chart(
            latency_chart_canvas,
            {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        { label: 'p50', data: [], borderWidth: 2, borderColor: "#a8c7f7", backgroundColor: "#5490F077", pointRadius: 1, pointHoverRadius: 12 },
                        { label: 'p90', data: [], borderWidth: 2, borderColor: "#a8c7f7", backgroundColor: "#295eb377", pointRadius: 1, pointHoverRadius: 12 },
                        { label: 'p99.9', data: [], borderWidth: 2, borderColor: "#a8c7f7", backgroundColor: "#0b156e77", pointRadius: 1, pointHoverRadius: 12 },
                    ]
                },
                options: {
                    scales: { 
                        x: { type: 'time', ticks: { maxTicksLimit: 2 }, grid: { color: '#ffffff33' }},
                        y: { ticks: { maxTicksLimit: 12 }, grid: { color: '#ffffff33' } },
                    },
                    color: '#e5e7eb',
                    animation: false,
                    fill: true,
                    maintainAspectRatio: false,
                    responsive: true,
                }
            }
        );
    })

    $effect(() => {
        if (!chart) return;
        chart.data.labels = latency_arrival_times;
        chart.data.datasets[0].data = [...p50s];
        chart.data.datasets[1].data = [...p90s];
        chart.data.datasets[2].data = [...p999s];

        chart.update();
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

<div class="w-full flex flex-col items-center bg-mist-900 text-white">
    <header class="flex justify-between items-center bg-mist-950 p-4 mb-4 w-full">
        <h1 class="text-lg sm:text-5xl font-black">
            Live Exchange View
        </h1>
        <div class="flex items-center justify-between gap-2 font-bold text-sm sm:text-xl">
            {#if exchangeConnected == true}
                <div class="bg-green-500 border border-gray-300 rounded-3xl w-3 h-3"></div> Connected
            {:else}
                <div class="bg-red-500 border border-gray-300 rounded-3xl w-3 h-3"></div> Disconnected
            {/if}
        </div>
    </header>
    <div class="flex flex-col justify-around items-center gap-8 px-4 w-full lg:w-3/4 2xl:w-2/5">
                <div class="flex flex-col w-full h-[40vh] rounded-md overflow-clip">
            <div class="flex flex-col sm:flex-row justify-between items-center gap-4 w-full p-4 bg-secondary-200 text-secondary-700">
                <h2 class="text-3xl">
                    Transactions
                </h2>
                <div class="flex flex-initial gap-1 md:gap-2 border-2 rounded-xl px-2 py-1 bg-slate-900 text-amber-400">
                    <p class="font-light">TPS</p> 
                    <p class="font-bold text-2xl sm:text-4xl">{tps100.toFixed(1)}</p>
                </div>
            </div>
            <Table shadow striped={true} hoverable={true} divClass="w-full no-scrollbar border-2 border-secondary-200 rounded-none sm:rounded-none" color="secondary">
                <TableHead>
                    <TableHeadCell>Time</TableHeadCell>
                    <TableHeadCell>Pair</TableHeadCell>
                    <TableHeadCell>Price</TableHeadCell>
                    <TableHeadCell>Vol.</TableHeadCell>
                    <TableHeadCell>Taker</TableHeadCell>
                    <TableHeadCell>dir.</TableHeadCell>
                    <TableHeadCell>Maker</TableHeadCell>
                </TableHead>
                <TableBody>
                {#each last100Transactions as transaction }
                    <TableBodyRow>
                        <TableBodyCell class="w-1/5 font-light">
                            {transaction.timestamp.toLocaleTimeString()}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/5">
                            {getMarketSymbolic(transaction.pair)}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/6">
                            {transaction.price.toFixed(2)}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/8">
                            {transaction.volume}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/8">
                            {transaction.taker}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/6">
                            {#if transaction.taker_side == 0}
                            <p class="text-green-500">buy</p>
                            {:else}
                            <p class="text-red-500">sell</p>
                            {/if}
                        </TableBodyCell>
                        <TableBodyCell class="w-1/8">
                            {transaction.maker}
                        </TableBodyCell>
                    </TableBodyRow>
                    {/each}
                </TableBody>
            </Table>
        </div>
        <div class="flex flex-col w-full overflow-clip rounded-md">
            <div class="flex items-center w-full p-4 bg-secondary-200 text-secondary-700">
                <h2 class="text-3xl">
                    Markets
                </h2>
            </div>
            <Table divClass="w-full no-scrollbar border-2 border-secondary-200" color="secondary">
                <TableHead>
                    <TableHeadCell>Symbol</TableHeadCell>
                    <TableHeadCell class="hidden sm:block text-left">Volume</TableHeadCell>
                    <TableHeadCell class="text-right">Bid</TableHeadCell>
                    <TableHeadCell class="text-left">Ask</TableHeadCell>
                    <TableHeadCell class="hidden sm:block text-right">Volume</TableHeadCell>
                </TableHead>
                <TableBody>
                    {#each exchangeState.l1s as marketL1 (`${marketL1.pair.primary},${marketL1.pair.secondary}`) }
                        <TableBodyRow>
                            <TableBodyCell>
                                {getMarketSymbolic(marketL1.pair)}
                            </TableBodyCell>
                            <TableBodyCell class="hidden sm:block text-left text-green-500 tabular-nums">
                                {#if marketL1.orderbook.best_bid != null}
                                {marketL1.orderbook.best_bid.volume}
                                {:else}
                                -
                                {/if}
                            </TableBodyCell>
                            <TableBodyCell class="text-right font-bold text-green-500 tabular-nums">
                                {#if marketL1.orderbook.best_bid != null}
                                {marketL1.orderbook.best_bid.price.toFixed(2)}
                                {:else}
                                -
                                {/if}
                            </TableBodyCell>
                            <TableBodyCell class="text-left font-bold text-red-500 tabular-nums">
                                {#if marketL1.orderbook.best_ask != null}
                                {marketL1.orderbook.best_ask.price.toFixed(2)}
                                {:else}
                                -
                                {/if}
                            </TableBodyCell>
                            <TableBodyCell class="hidden sm:block text-right text-red-500 tabular-nums">
                                {#if marketL1.orderbook.best_ask != null}
                                {marketL1.orderbook.best_ask.volume}
                                {:else}
                                -
                                {/if}
                            </TableBodyCell>
                        </TableBodyRow>
                    {/each}
                </TableBody>
            </Table>
        </div>
        <div class="flex flex-col items-center w-full h-[50vh]">
            <div class="flex flex-col sm:flex-row justify-between items-center gap-4 w-full p-4 bg-secondary-200 text-secondary-700">
                <h2 class="text-4xl">
                    Command Latency (ms)
                </h2>
                <div class="flex items-start gap-1 md:gap-2 border-2 rounded-xl px-2 py-1 bg-slate-900 text-amber-400">
                    <p class="font-light text-xs md:text-md">p50</p>
                    <p class="font-bold text-xl sm:text-4xl tabular-nums">{exchangeState.metrics.p50.toFixed(2)}</p>
                    <p class="font-light text-xs md:text-md">p90</p>
                    <p class="font-bold text-xl sm:text-4xl tabular-nums">{exchangeState.metrics.p90.toFixed(2)}</p>
                    <p class="font-light text-xs md:text-md">p99.9</p>
                    <p class="font-bold text-xl sm:text-4xl tabular-nums">{exchangeState.metrics.p999.toFixed(2)}</p>
                </div>
            </div>
            <div class="relative w-full h-5/6 flex-1 border-2 border-secondary-200 bg-slate-900">
                <canvas bind:this={latency_chart_canvas}></canvas>
            </div>
        </div>
    </div>
    <div class="flex w-full justify-between mt-4 py-4 px-4 bg-mist-950">
        <div>
            (C) Teun van Wezel
        </div>
        <div>
            Footer 
        </div>
    </div>
</div>



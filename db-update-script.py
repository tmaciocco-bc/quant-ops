
import time
import pandas as pd

# virtualenv enviroment_name -p python3

SECURITY_EXCHANGE = {
    "BINANCEUS": "M",
    "BITFINEX": "G",
    "BLOCKCHAIN": "E",
    "CUMBERLAND": "J",
    "LMAX": "K",
}

TRADING_GATEWAY = {
    "BINANCEUS": "liquidity-trading-gateway-binance-us",
    "BITFINEX": "liquidity-trading-gateway-bitfinex",
    "BLOCKCHAIN": "liquidity-trading-gateway-blockchain",
    "CUMBERLAND": "liquidity-hybrid-gateway-cumberland",
    "LMAX": "liquidity-trading-gateway-lmax-fx",
}

# TO-DO
#   log these each time we update the db
#   pip3 / python3


def write_db_procedures(procedure_type, client_order_id, algo_service_name, isin,
                       order_price, order_qty, avg_px, cum_qty, leaves_qty, exchange_client_order_id, order_id, tif,
                        created_at, updated_at=int(time.time() * 1e6), complete=1, order_status=5, reason=""):
    """
    procedure_type :: 'insert_order' or 'update_order'
    client_order_id :: from agw / ogw
    exchange_client_order_id :: use clorid from mercury_fills if done on-venue
    order_id ::
    """

    avail_procedures = {'insert_order', 'update_order'}
    if procedure_type not in avail_procedures:
        raise ValueError("procedure_type must be in %s" % avail_procedures)


    # ie "BLOCKCHAIN_BTCUSD" -> "BLOCKCHAIN"
    venue = isin.split("_")[0]
    security_exch = SECURITY_EXCHANGE[venue]
    tgw = TRADING_GATEWAY[venue]

    agw = """["request","liquidity-account-gateway",
    "{\\"event\\": \\"update\\",
    \\"type\\": \\"storedprocedure\\",
    \\"data\\": [{\\"name\\": \\"%s\\",
    \\"parameters\\": 
    \\"val_client_order_id=>'%s', 
    val_algorithm_service_name=>'%s', 
    val_security_exchange=>'%s', 
    val_isin=>'%s', 
    val_order_type=>'0', 
    val_order_price=>'%.8f', 
    val_order_qty=>'%.8f', 
    val_order_status=>'%i', 
    val_leaves_qty=>'%.8f', 
    val_cum_qty=>'%.8f', 
    val_complete=>'%i', 
    val_created_at=>'%i',
    val_updated_at=>'%i', 
    val_avg_px=>'%.8f'\\"}]}"]""" % (
        procedure_type,
        client_order_id,
        algo_service_name,
        security_exch,
        isin,
        order_price,
        order_qty * 10000 if venue == 'LMAX' else order_qty,
        order_status,
        leaves_qty * 10000 if venue == 'LMAX' else leaves_qty,
        cum_qty * 10000 if venue == 'LMAX' else cum_qty,
        complete,
        created_at,
        updated_at,
        avg_px
    )

    agw = agw.replace("\n", "").replace(" ","")

    ogw = """["request","liquidity-order-gateway",
    "{\\"event\\": \\"update\\",
    \\"type\\": \\"storedprocedure\\",
    \\"data\\": [{\\"name\\": \\"%s\\",
    \\"parameters\\":
    \\"val_client_order_id=>'%s',
    val_trading_gateway_service_name=>'%s',
    val_algorithm_service_name=>'%s',
    val_security_exchange=>'%s',
    val_exchange_client_order_id=>'%s',
    val_isin=>'%s',
    val_order_type=>'0',
    val_order_price=>'%.8f',
    val_order_qty=>'%.8f',
    val_time_in_force=>'%i',
    val_order_status=>'%i',
    val_order_id=>'%s',
    val_leaves_qty=>'%.8f',
    val_cum_qty=>'%.8f',
    val_avg_px=>'%.8f',
    val_last_price=>'%.8f',
    val_last_qty=>'%.8f',
    val_complete=>'%i',
    val_created_at=>'%i',
    val_updated_at=>'%i'\\"}]}"]""" % (
        procedure_type,
        client_order_id,
        tgw,
        algo_service_name,
        security_exch,
        exchange_client_order_id,
        isin,
        order_price,
        order_qty,
        tif,
        order_status,
        order_id,
        leaves_qty,
        cum_qty,
        avg_px,
        avg_px, # last_price
        cum_qty, # last_qty
        complete,
        created_at,
        updated_at)

    ogw = ogw.replace("\n", "").replace(" ","")

    return "{0}\n\n{1}\n\n{0}\n\n{2}".format("*" * 100, agw, ogw)


if __name__ == "__main__":
    print(write_db_procedures(procedure_type="insert_order",
                        client_order_id="b98a8739-20e4-4651-8630-7134adca2b78_manual_leg",
                        algo_service_name="liquidity-algo-arb-btc-usd-bfx-binus",
                        isin="BITFINEX_BTCUSD",
                        order_price=34826.000000000,
                        order_qty=-0.001200000,
                        avg_px=34826.000000000,
                        cum_qty=-0.001200000,
                        leaves_qty=0.000000000,
                        created_at=1611950517441362,
                        # updated_at=1611248896201110,
                        exchange_client_order_id='UNKNOWN',
                        order_id='UNKNOWN',
                        tif=3,
                        order_status=3,
                        reason=""))

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flink_types import Collector

# Sales transform logic — Python implementation

def process_element(line: str, out: Collector) -> None:
    """
    Parses a raw sales CSV line and emits an enriched CSV string via out.collect(),
    which is the Flink Collector<String> passed in from Java.
    Lines that are blank, headers, or malformed are silently dropped.

    Input  fields (6): transactionId, customerId, product, quantity, unitPrice, date
    Output fields (8): transactionId, customerId, product, quantity, unitPrice,
                       totalPrice, category, date
    """
    if not line.strip() or line.startswith('transactionId'):
        return

    fields = line.split(',')
    if len(fields) != 6:
        return

    try:
        transaction_id = fields[0].strip()
        customer_id    = fields[1].strip()
        product        = fields[2].strip()
        quantity       = int(fields[3].strip())
        unit_price     = float(fields[4].strip())
        date           = fields[5].strip()

        total_price = quantity * unit_price

        if total_price < 100.0:
            category = 'small'
        elif total_price < 500.0:
            category = 'medium'
        else:
            category = 'large'

        out.collect(f"{transaction_id},{customer_id},{product},{quantity},"
                    f"{unit_price:.2f},{total_price:.2f},{category},{date}")

    except (ValueError, IndexError):
        return

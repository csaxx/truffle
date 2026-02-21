# Sales transform logic — Python implementation

def process_element(line):
    """
    Parses a raw sales CSV line and returns an enriched CSV string, or None
    if the line should be skipped (header, blank, malformed).

    Input  fields (6): transactionId, customerId, product, quantity, unitPrice, date
    Output fields (8): transactionId, customerId, product, quantity, unitPrice,
                       totalPrice, category, date
    """
    if not line.strip() or line.startswith('transactionId'):
        return None

    fields = line.split(',')
    if len(fields) != 6:
        return None

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

        return (f"{transaction_id},{customer_id},{product},{quantity},"
                f"{unit_price:.2f},{total_price:.2f},{category},{date}")

    except (ValueError, IndexError):
        return None

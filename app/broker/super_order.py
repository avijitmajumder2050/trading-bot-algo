# app/broker/super_order.py

class SuperOrder:
    def __init__(self, dhan_context):
        self.dhan_http = dhan_context.get_dhan_http()

    def place_super_order(
        self,
        security_id,
        exchange_segment,
        transaction_type,
        quantity,
        order_type,
        product_type,
        price,
        targetPrice=0.0,
        stopLossPrice=0.0,
        trailingJump=0.0,
        tag=None
    ):
        payload = {
            "transactionType": transaction_type.upper(),
            "exchangeSegment": exchange_segment.upper(),
            "productType": product_type.upper(),
            "orderType": order_type.upper(),
            "securityId": str(security_id),
            "quantity": int(quantity),
            "price": float(price),
            "targetPrice": float(targetPrice),
            "stopLossPrice": float(stopLossPrice),
            "trailingJump": float(trailingJump)
        }

        if tag:
            payload["correlationId"] = tag

        return self.dhan_http.post("/super/orders", payload)

    def modify_super_order(
        self,
        order_id,
        order_type,
        leg_name,
        quantity=0,
        price=0.0,
        targetPrice=0.0,
        stopLossPrice=0.0,
        trailingJump=0.0
    ):
        payload = {"orderId": order_id, "legName": leg_name}

        if leg_name == "ENTRY_LEG":
            payload.update({
                "orderType": order_type,
                "quantity": int(quantity),
                "price": float(price),
                "targetPrice": float(targetPrice),
                "stopLossPrice": float(stopLossPrice),
                "trailingJump": float(trailingJump)
            })

        elif leg_name == "STOP_LOSS_LEG":
            payload.update({
                "stopLossPrice": float(stopLossPrice),
                "trailingJump": float(trailingJump)
            })

        elif leg_name == "TARGET_LEG":
            payload.update({"targetPrice": float(targetPrice)})

        return self.dhan_http.put(f"/super/orders/{order_id}", payload)

    def cancel_super_order(self, order_id, leg):
        return self.dhan_http.delete(f"/super/orders/{order_id}/{leg}")

# OrderEventProcessor
My solution for the interview project at Gen.

## Running project
1. Run the `compose.yaml` as:
` > docker-compose -f compose.yaml up`
To establish docker image for RabbitMq and Postgresql

2. Run the .NET project either from command line:
` > dotnet build `
and
`> dotnet run`
Or from the Visual Studio envinronment.

3. Send testing messages either via prepared script or via RabbitMq Web Interface (see more info below)


## Format of the messages
Messages are send via one queue name **order_queue** and they are using routing key(!!!) **order**.
### Format of OrderEvent
```
Headers: X-MsgType: OrderEvent
Payload:
{
  "id": "0-111",
  "product": "PR-ABC",
  "total": "12,34",
  "currency": "CZK"
}
```

### Format of PaymentEvent
```
Headers: X-MsgType: PaymentEvent
Payload:
{
  "orderId": "0-111",
  "amount": "12,34"
}
```

## Known Issues
There might be problems with localization (?) which makes the decimal point being ',', which may end up in an error message and the program crashing.
As of now, the *Status* column has only three states – **PAID**, **PARTIALLY PAID** and **''**.

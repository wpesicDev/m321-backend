# Protocol definition

## Motivation

The protocol aims to minimize computational overhead of data-exchange between low-performance-computers and clients while still enabling robust error handling and data-typing.

## Terms

| Term         | Definition                                                                                                                                           |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| server       | The party wich provides the data to clients.                                                                                                         |
| client       | The party wich requests the data.                                                                                                                    |
| data-keyword | A short name (with no semicolons) wich uniquely identifies data requested from a server. Every server may specify the data-keywords wich it accepts. |
| error-code   | A Numeric code (`[0-9]+`) wich uniquely identifies an error from a server. Every server may specify its own error-codes.                             |

## Format

All the exchanged strings are to be encoded in UTF-8.<br>
A request can contain one or more data-keywords seperated by semicolons (;). No space-characters are allowed as seperation.

```
<DATA_KEYWORD>[;<DATA_KEYWORD>]...
```

In response to such a request a server either responds with an erroneous response (error) or with the data requested.<br>
A **positive response** is a JSON-compliant list wich contains return values in the same order wich they have been requested in.

```
\[<DATA>[,<DATA>]\]
```

Note that the server defines the data-types of the requested data so the type of `<DATA>` is set by the server.<br>
Thus on a protocol-level-view the type of a successful response is `list[<Any-JSON-Type>]`. It is however the duty of the server to define the data-types of the responses.

A negative aka. **erroneous response** has the following structure:

```
e<ERROR_CODE>
```

The first byte or character of an erroneous response MUST be the letter 'e' indication an error. The following is the [error-code](#terms)

### Error handling

A client can tell erroneous and successful responses apart by inspecting the first byte of the response.<br>
If the byte is an 'e' then an error occured, if the first byte is the character '[' then the response is a successful one.

## Means of data transportation

The client and server send the afore mentioned protocol-data via a TCP/IP socket.<br>
This socket may be kept open for an arbitrary amount of time, thus enabling the processing of multiple requests.<br>
So one socket-connection may be used to send multiple requests and receive multiple responses.

# Examples

The examples assume that the server defines the following data-keywords and error-codes.

| data-keyword | data-type | comment             |
| ------------ | --------- | ------------------- |
| temp         | float     | temperature in C°   |
| humi         | int       | humidity in percent |

| error-code | meaning                                   |
| ---------- | ----------------------------------------- |
| 1          | A requested data-keyword does not exist.  |
| 2          | A processing error occured in the server. |

## Singe Data-Keyword

A client sends the following string to the server to request the temperature:

```
temp
```

Then the server may respond with:

```
[42.1]
```

Meaning that it is 42.1 C° at the sensor wich the server used to measure the temperature.

## Multi Data-Keyword

A client sends the following string to the server to request the temperature and the humidity.

```
temp;humi
```

Then the server may respond with:

```
[42.1,100]
```

Meaning that the server must be in a tropical environment where it is 42.1 C° and the environment has a humidity of 100 percent.<br>

If the client sends the data-keywords in reverse order.

```
humi;temp
```

Then the response will be reversed in order as well:

```
[100,42.1]
```

## Erroneous Response 1

A client requests the temperature from the server:

```
temp
```

The server answers with an erroneous response like:

```
e2
```

Meaning that the server encounterd an error while processing the request/response.

## Erroneous Response 2

A client requests the meaning of life from from the server:

```
meaning_of_life
```

The server answers with an erroneous response like:

```
e1
```

Meaning that the server does not know of a data-keyword with the name "meaning_of_life".

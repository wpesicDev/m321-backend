# Validity
The following applies to both of the M5-S3-Stacks of Team-1 (Sensor-Layer).

# Data the Sensor-Layer provides

| name         | description                      |
| ------------ | -------------------------------- |
| Image        | current image of the sky         |
| Temperature  | current temperature from sensor  |
| Humidity     | current humidity from sensor     |
| Air Pressure | current air pressure from sensor |
| Luminosity   | current luminosity from sensor   |

## Data-Keywords

These keywords are defines as per the [protocol definition](./protocol.md#protocol-definition).<br>
The "name" column corresponds to the data-names in the previous table.

| name         | keyword | datatype | unit of measurement | comment                                      |
| ------------ | ------- | -------- | ------------------- | -------------------------------------------- |
| Image        | img     | str      | -                   | image is a base64-encoded <TODO: image-type> |
| Temperature  | temp    | float    | C°                  |                                              |
| Humidity     | humi    | float    | % RH                |                                              |
| Air Pressure | airp    | int      | Pascal              |                                              |
| Luminosity   | lum     | int      | Lux                 | Is not measured in mister Lux                |

# Error codes

## Overview

| range   | error-source   |
| ------- | -------------- |
| 0-99    | General/System |
| 100-199 | Image          |
| 200-299 | Temperature    |
| 300-399 | Humidity       |
| 400-499 | AirPressure    |
| 500-599 | Luminosity     |

## General/System Errors

| code | meaning                                                           |
| ---- | ----------------------------------------------------------------- |
| 0    | The request is malformed, not conforming to the protocol          |
| 1    | The requested data-keyword does not exist                         |
| 2    | The server does not have enough resources to complete the request |

## Image Errors

TBD

## Temperature Errors

TBD

## Humidity Errors

TBD

## AirPressure Errors

TBD

## Luminosity Errors

TBD

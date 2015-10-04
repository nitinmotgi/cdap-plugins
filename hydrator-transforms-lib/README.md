## Cask Hydrator Transform Plugin Collections.

![alt tag](https://github.com/nitinmotgi/hydrator-plugins/blob/master/hydrator-transforms-lib/resources/screenshot.png)

### CSVParser

CSVParser takes a input field and parses in to a CSV Record with CSV Parser. The CSVParser supports different CSV formats like DEFAULT, MYSQL, EXCEL, RFC4180 and TDF.

### CSVParser2
CSVParser takes a input field to parse it as CSV Record, but it now supports first the ability to decode the field using either BASE64, BASE32 or HEX and then apply decompression on the payload using SNAPPY and then parse the record. There are some use-cases where payloads are Compressed, Hex encoded and are CSV records. 

![CSVParser2 Plugin used to decode events from Kafka](https://github.com/nitinmotgi/hydrator-plugins/blob/master/hydrator-transforms-lib/resources/csvparser2-0.png)

![CSVParser2 Supports Decoder](https://github.com/nitinmotgi/hydrator-plugins/blob/master/hydrator-transforms-lib/resources/csvparser2-1.png)

![CSVParser2 Supports Decompression](https://github.com/nitinmotgi/hydrator-plugins/blob/master/hydrator-transforms-lib/resources/csvparser2-2.png)

### Masker
The Masker masks string field. Mask generated are of same length as the input field value. A seed is used to randomly select the characters that are used for Masking. 

### Hasher
The Hasher uses hashing algorithms to encode values of a field. Currently hasher supports MD2, MD5, SHA1, SHA256, SHA384, SHA512 algorithms for hashing a field. It's mainly used for encoding sensitive data like credit card numbers, social security numbers, and PII fields.

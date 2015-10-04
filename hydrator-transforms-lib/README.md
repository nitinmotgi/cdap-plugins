## Cask Hydrator Transform Plugin Collections.

![alt tag](https://github.com/nitinmotgi/hydrator-plugins/blob/master/hydrator-transforms-lib/resources/screenshot.png)


### Build Instructions
```
$ git clone https://github.com/nitinmotgi/hydrator-plugins.git
$ cd hydrator-plugins
$ export HYDRATOR_PLUGIN=`pwd`
$ mvn clean package
```

### Deploying Artifact to CDAP

You can use CDAP command line interface to deploy the hydrator plugin artifacts to CDAP. Once, the artifact is deployed it would be available in hydrator as a plugin under "Transforms" panel. 

#### Deploy Hydrator Plugins Artifact to CDAP
```
$ cdap (http://Joltie:10000/default)> load artifact $HYDRATOR_PLUGIN/target/hydrator-transforms-lib-1.0-SNAPSHOT.jar \
--config-file $HYDRATOR_PLUGIN/resources/plugin/hydrator-transforms-lib.json
$ cdap (http://Joltie:10000/default)> list artifacts
```

#### Deploy UI Configuration
```
$ cp $HYDRATOR_PLUGIN/resources/ui/*.json $CDAP_HOME/ui/templates/common
```

#### Update Hydrator Transform Plugins Artifact

In case you make any modifications to the hydrator-plugins JAR and you have not modified the version number, you would have delete the artifact and load the latest one.

```
$ cdap (http://Joltie:10000/default)> delete artifact hydrator-transforms-lib 1.0-SNAPSHOT
$ cdap (http://Joltie:10000/default)> load artifact $HYDRATOR_PLUGIN/target/hydrator-transforms-lib-1.0-SNAPSHOT.jar \
--config-file $HYDRATOR_PLUGIN/resources/plugin/hydrator-transforms-lib.json
$ cdap (http://Joltie:10000/default)> list artifacts
```

## Available Plugins

### CSVParser

CSVParser takes a input field and parses in to a CSV Record with CSV Parser. The CSVParser supports different CSV formats like DEFAULT, MYSQL, EXCEL, RFC4180 and TDF.

### CSVParser2
CSVParser takes a input field to parse it as CSV Record, but it now supports first the ability to decode the field using either BASE64, BASE32 or HEX and then apply decompression on the payload using SNAPPY, GIP or ZIP algorithms and then parse the record as CSV. There are some use-cases where payloads are Compressed, Hex encoded and are CSV records. 


<img src="https://raw.githubusercontent.com/nitinmotgi/hydrator-plugins/master/hydrator-transforms-lib/resources/csvparser2-0.png" width="400"><img src="https://raw.githubusercontent.com/nitinmotgi/hydrator-plugins/master/hydrator-transforms-lib/resources/csvparser2-1.png" width="400"><img src="https://raw.githubusercontent.com/nitinmotgi/hydrator-plugins/master/hydrator-transforms-lib/resources/csvparser2-2.png" width="400">

### Masker
The Masker masks string field. Mask generated are of same length as the input field value. A seed is used to randomly select the characters that are used for Masking. 

### Hasher
The Hasher uses hashing algorithms to encode values of a field. Currently hasher supports MD2, MD5, SHA1, SHA256, SHA384, SHA512 algorithms for hashing a field. It's mainly used for encoding sensitive data like credit card numbers, social security numbers, and PII fields.

### Clone Row 
The Clone Row transform creates copies or clones of a every row passed through and outputs them directly after the original row to the next stages of the pipeline.

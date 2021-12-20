from pprint import pformat

from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter, log


class ParkingLockConverter(BLEUplinkConverter):
    def __init__(self, config):
        self.__config = config
        log.info(self.__config)
        self.dict_result = {"deviceName": config.get('name', config['MACAddress']),
                            "deviceType": config.get('deviceType', 'BLEDevice'),
                            "telemetry": [],
                            "attributes": []
                            }

    def convert(self, config, data, to_string=False):
        try:
            if config.get('clean', True):
                self.dict_result["telemetry"] = []
                self.dict_result["attributes"] = []
            try:
                byte_from = config['section_config'].get('byteFrom')
                byte_to = config['section_config'].get('byteTo')
                try:
                    if data is None:
                        return {}
                    byte_to = byte_to if byte_to != -1 else len(data)
                    converted_data = data[byte_from: byte_to]
                    if to_string is True:
                        try:
                            converted_data = converted_data.replace(b"\x00", b'').decode('UTF-8')
                        except UnicodeDecodeError:
                            converted_data = str(converted_data)
                    else:
                        converted_data = int.from_bytes(converted_data, "big", signed=False)

                    if config['section_config'].get('key') is not None:
                        self.dict_result[config['type']].append({config['section_config'].get('key'): converted_data})
                    else:
                        log.error('Key for %s not found in config: %s', config['type'], config['section_config'])
                except Exception as e:
                    log.error('\nException catched when processing data for %s\n\n', pformat(config))
                    log.exception(e)
            except Exception as e:
                log.exception(e)
        except Exception as e:
            log.exception(e)
        log.debug(self.dict_result)
        return self.dict_result

import struct

from pprint import pformat

from thingsboard_gateway.connectors.ble.ble_uplink_converter import BLEUplinkConverter, log

KIN_MODEL = 'kinouwell'
KIN_STS_LEN = 27
KIN_HEAD_HIGH = 0xaa
KIN_HEAD_LOW = 0xbb
KIN_STS_KEY = "status_raw"

class ParkingLockConverter(BLEUplinkConverter):
    def __init__(self, config):
        self.__config = config
        log.info(self.__config)
        self.dict_result = {"deviceName": config.get('name', config['MACAddress']),
                            "deviceType": config.get('deviceType', 'BLEDevice'),
                            "telemetry": [],
                            "attributes": []
                            }
        self.model = config.get('model', '')
        self.support_model = [KIN_MODEL]

    def convert_kinouwell(self, key:str, uncovert_data:bytes) -> dict:
        rslt_dict = {}
        sts_len = len(uncovert_data)
        if key == KIN_STS_KEY:
            if sts_len == KIN_STS_LEN:
                if uncovert_data[0] == KIN_HEAD_HIGH and uncovert_data[1] == KIN_HEAD_LOW:
                    header, pkt_len, pkt_cnt, dev_type, pkt_type, dev_id, ex_pwd,\
                    hb, bat_lv, signal_str, park_sts, err_sts, coil_freq, arm_sts,\
                    chk_sum = struct.unpack(">HBBBBLLHHBBHHHB", uncovert_data)
                    rslt_dict["dev_id"] = dev_id
                    rslt_dict["battery"] = bat_lv
                    rslt_dict["sts_val"] = park_sts
                    rslt_dict["fault_val"] = err_sts
                else:
                    log.error("Invalid header")
            else:
                log.error("Invalid status len, expect:{}, got:{}".format(KIN_STS_LEN, sts_len))
        else:
            converted_data = int.from_bytes(uncovert_data, "big", signed=False)
            rslt_dict = {key: converted_data}

        return rslt_dict

    def convert(self, config, data, to_string=False):
        try:
            if config.get('clean', True):
                self.dict_result["telemetry"] = []
                self.dict_result["attributes"] = []
            try:
                if config['section_config'].get('key') is not None:
                    byte_from = config['section_config'].get('byteFrom')
                    byte_to = config['section_config'].get('byteTo')
                    try:
                        if data is None:
                            return {}
                        byte_to = byte_to if byte_to != -1 else len(data)
                        raw_data = data[byte_from: byte_to]
                        if to_string is True:
                            try:
                                converted_data = raw_data.replace(b"\x00", b'').decode('UTF-8')
                            except UnicodeDecodeError:
                                converted_data = str(raw_data)
                                rslt_dict = {config['section_config'].get('key'): converted_data}
                        else:
                            if self.model in self.support_model:
                                if self.model == KIN_MODEL:
                                    rslt_dict = self.convert_kinouwell(config['section_config'].get('key'), raw_data)
                            else:
                                converted_data = int.from_bytes(raw_data, "big", signed=False)
                                rslt_dict = {config['section_config'].get('key'): converted_data}
                        if len(rslt_dict) > 0:
                            self.dict_result[config['type']].append(rslt_dict)
                    except Exception as e:
                        log.error('\nException catched when processing data for %s\n\n', pformat(config))
                        log.exception(e)
                else:
                    log.error('Key for %s not found in config: %s', config['type'], config['section_config'])

            except Exception as e:
                log.exception(e)
        except Exception as e:
            log.exception(e)
        log.debug(self.dict_result)
        return self.dict_result

import logging
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from os.path import dirname

from fuzzywuzzy import process, fuzz

from vnaddress.functions.acronym_layer import acronym_execute_raw_address, acronym_execute_list_address
from .constants import ERROR_DICT, STANDARD_ADDRESSES, STANDARD_SINGLE_NAMES, FULL_ADDRRESS_ERROR_DICT
from .functions.crf_layer import crf_execute
from .functions.crf_layer.constants import FULL_TAG_LIST
from .functions.crf_layer.crf_execute import list2sentence, extract_relative_tag
from .functions.spell_correction_layer import match_word, match_sentence
from .functions.utils import sentence2list, list_subtract

COMMA_HANDLE = False
BASE_DIR = dirname(dirname(__file__))
STANDARDIZER_MODEL_PATH = 'vnaddress/models'


class VNAddressStandardizer(object):
    def __init__(self, raw_address, comma_handle, detail = False):
        self.raw_address = raw_address
        self.comma_handle = comma_handle
        self.detail = detail

    def combined_processing(self, raw_address, comma_handle=COMMA_HANDLE, scorer = fuzz.partial_ratio):
        error_dict = ERROR_DICT
        standard_addresses = STANDARD_ADDRESSES
        full_address_error_dict = FULL_ADDRRESS_ERROR_DICT
        standard_single_names = STANDARD_SINGLE_NAMES

        # logger.info(raw_address)
        lowered_address = raw_address.lower()
        # logger.info(acronym_fixed_address)

        if comma_handle:
            # address2list = sentence2list(lowered_address)
            # acronym_fixed_address = acronym_execute_list_address(address2list)
            # pool = mp.Pool(mp.cpu_count())
            # corrected_address2list = pool.starmap(match_word,
            #                                       [(error_dict, name, standard_single_names) for name in
            #                                        acronym_fixed_address])
            # pool.close()
            address2list = sentence2list(lowered_address)
            acronym_fixed_address = acronym_execute_list_address(address2list)

            # Instead of using a multiprocessing pool, use a list comprehension or a loop to apply match_word function
            corrected_address2list = [match_word(error_dict, name, standard_single_names) for name in acronym_fixed_address]

        else:
            acronym_fixed_address = acronym_execute_raw_address(lowered_address)
            corrected_address = match_sentence(full_address_error_dict, acronym_fixed_address, standard_addresses)
            corrected_address2list = sentence2list(corrected_address)
            # logger.info(corrected_address2list)

        crf_result = crf_execute("{}/{}/finalized_model.sav".format(BASE_DIR, STANDARDIZER_MODEL_PATH), corrected_address2list)

        tag_list = [extract_relative_tag(item[1]) for item in crf_result]
        missing_tag_list = list_subtract(FULL_TAG_LIST, tag_list)
        # logger.info(missing_tag_list)

        name_only_crf_result = ", ".join([item[0] for item in crf_result])
        match_check = process.extractOne(name_only_crf_result, standard_addresses, scorer=scorer)

        return crf_result, match_check, missing_tag_list

    def execute(self):
        try:
            raw_address = self.raw_address
            detail = self.detail
            comma_handle = self.comma_handle if self.comma_handle else COMMA_HANDLE
            result, match_check, missing_tag_list = self.combined_processing(raw_address, comma_handle)

            if match_check[1] < 100:
                result, match_check, _ = self.combined_processing(match_check[0], comma_handle, scorer=fuzz.partial_ratio)
                if "PX" in missing_tag_list:
                    result = result[1:]
                    if "QH" in missing_tag_list:
                        result = result[1:]

            result_data = {
                "result": list2sentence(result),
                "match": {
                    "match_address": match_check[0],
                    "match_percent": match_check[1]
                },
                "missing": missing_tag_list,
                "detail": {extract_relative_tag(item[1]): item[0] for item in result}
            }

        except Exception as e:
            logging.error('Error at', exc_info=e)
        else:
            if detail:
                print(result_data)
            else:
                print(list2sentence(result))

    def combined_processing_list(self, raw_address, comma_handle=COMMA_HANDLE, scorer = fuzz.partial_ratio):
        error_dict = ERROR_DICT
        standard_addresses = STANDARD_ADDRESSES
        full_address_error_dict = FULL_ADDRRESS_ERROR_DICT
        standard_single_names = STANDARD_SINGLE_NAMES

        # logger.info(raw_address)
        lowered_address = raw_address.lower()
        # logger.info(acronym_fixed_address)

        if comma_handle:
            address2list = sentence2list(lowered_address)
            acronym_fixed_address = acronym_execute_list_address(address2list)
            pool = ThreadPool()
            corrected_address2list = pool.starmap(match_word,
                                                  [(error_dict, name, standard_single_names) for name in
                                                   acronym_fixed_address])
            pool.close()
            # address2list = sentence2list(lowered_address)
            # acronym_fixed_address = acronym_execute_list_address(address2list)

            # # Instead of using a multiprocessing pool, use a list comprehension or a loop to apply match_word function
            # corrected_address2list = [match_word(error_dict, name, standard_single_names) for name in acronym_fixed_address]

        else:
            acronym_fixed_address = acronym_execute_raw_address(lowered_address)
            corrected_address = match_sentence(full_address_error_dict, acronym_fixed_address, standard_addresses)
            corrected_address2list = sentence2list(corrected_address)
            # logger.info(corrected_address2list)

        crf_result = crf_execute("{}/{}/finalized_model.sav".format(BASE_DIR, STANDARDIZER_MODEL_PATH), corrected_address2list)

        tag_list = [extract_relative_tag(item[1]) for item in crf_result]
        missing_tag_list = list_subtract(FULL_TAG_LIST, tag_list)
        # logger.info(missing_tag_list)

        name_only_crf_result = ", ".join([item[0] for item in crf_result])
        match_check = process.extractBests(name_only_crf_result, standard_addresses, scorer=scorer)

        return crf_result, match_check, missing_tag_list

    def execute_list(self):
        match_check_list = []
        try:
            raw_address = self.raw_address
            detail = self.detail
            comma_handle = self.comma_handle if self.comma_handle else COMMA_HANDLE
            result, match_check, missing_tag_list = self.combined_processing_list(raw_address, comma_handle)

            # if match_check[0][1] < 100:
            #     result, match_check, _ = self.combined_processing_list(match_check[0][0], comma_handle, scorer=fuzz.partial_ratio)
            #     if "PX" in missing_tag_list:
            #         result = result[1:]
            #         if "QH" in missing_tag_list:
            #             result = result[1:]
            
            for match in match_check:
                match_data = {
                    "match_address" : match[0],
                    "match_percent" : match[1]
                }
                match_check_list.append(match_data)
            list
            result_data = {
                "result": list2sentence(result),
                "match": match_check_list,
                "missing": missing_tag_list,
                "detail": {extract_relative_tag(item[1]): item[0] for item in result}
            }
        except Exception as e:
            logging.error('Error at', exc_info=e)
        
        return match_check_list
        

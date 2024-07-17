from vnaddress import VNAddressStandardizer
import unicodedata

# Two example strings
s1 = " Ba Đình, Hà Nội"  # likely in NFD
s2 = " Ba Đình, Hà Nội"  # likely in NFC

# Normalize to NFC
s1_normalized = unicodedata.normalize('NFC', s1)
s2_normalized = unicodedata.normalize('NFC', s2)

address_standardizer = VNAddressStandardizer(raw_address=s1_normalized, comma_handle= True,detail=True)
result_list = address_standardizer.execute_list()
print(result_list)
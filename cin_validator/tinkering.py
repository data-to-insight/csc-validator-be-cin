import pandas as pd
import numpy as np

df = pd.DataFrame(
    {'UPN' : [pd.NA, 'X000000000000', 'X0000y0000000', 'x0000000er00e0']
})

UPNs = df[df['UPN'].notna()]

allowed_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9',]
s = UPNs['UPN'].str[4:12]

failing_indices = []
#for row, index in UPNs.iterrows():
#    s = index['UPN'][4:12]
#    if all(ch in allowed_list for ch in s) == False:
#        failing_indices.append(row)
#print(failing_indices)

print(any(~ch.isdigit() for ch in s))

#r = UPNs['UPN'].str[4:12]
#a = all(ch in allowed_list for ch in r)
#q = UPNs[UPNs['UPN']]

#print(all(ch in allowed_s for ch in s))
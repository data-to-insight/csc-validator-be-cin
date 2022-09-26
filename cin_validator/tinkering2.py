import pandas as pd
import numpy as np

df = pd.DataFrame(
    {'UPN' : [pd.NA, 'X000000000000', 'X0000y0000000', 'x0000000er00e0']
})

UPNs = df[df['UPN'].notna()]
UPNs['UPN'] = UPNs['UPN'].str[4:12]
UPNs['UPN'] = UPNs['UPN'].map(lambda x: x.strip('0123456789'))
failing_indices = UPNs[UPNs['UPN'].str.len() > 0].index

print(failing_indices)


# ds_toolkit
Tools for plotting and wrangling data

## How-to
There are two install options

### pip install method (recommended for production)
1. Run ```python -m pip install git+https://github.com/char8060/ds_toolkit```
2. Import ```from ds_toolkit.tools import *```

### git-clone method (recommended for development)
Add the following to your notebook or `.py` file to use the toolkit

```
import os
import sys
sys.path.insert(1, '<path_to_repo>/ds_toolkit')
from ds_toolkit.tools import *
```

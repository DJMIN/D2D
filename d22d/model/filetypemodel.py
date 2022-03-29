import magic
print(magic.from_file('./filetypemodel.py'))

import mimetypes
print(
mimetypes.guess_type('./filetypemodel.py')
)
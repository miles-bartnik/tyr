import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os

ep = ExecutePreprocessor(timeout=600, kernel_name="python3")

order = os.listdir(os.path.abspath(os.path.join(os.getcwd(), "..", "Tutorials")))

order.sort()

print(order)

for file in order:
    print(file)

    if file.split(".")[-1] == "ipynb":
        with open(
            os.path.abspath(os.path.join(os.getcwd(), "..", "Tutorials", file))
        ) as f:
            nb = nbformat.read(f, as_version=4)
            ep.preprocess(nb)
            f.close()

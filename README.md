# ライブラリ
```
pip install sphinx
pip install myst-parser
pip install furo
pip install sphinx-copybutton
pip install linkify-it-py
pip install sphinxcontrib-diagrams
```

# コマンド
```bash
cd docs/source
sphinx-apidoc -f -o  ./bid_optimisation_ml/sophia-ai ../../sophia-ai
sphinx-apidoc -f -o  ./bid_optimisation_ml/cpc_prediction ../../cpc_prediction
sphinx-apidoc -f -o  ./bid_optimisation_ml/cvr_prediction ../../cvr_prediction
sphinx-apidoc -f -o  ./bid_optimisation_ml/spa_prediction ../../spa_prediction
sphinx-apidoc -f -o  ./bid_optimisation_ml/common_module ../../common_module


```
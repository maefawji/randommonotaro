# Webpage Viewer

MonotaRO のカテゴリ一覧 CSV をローカルに持ち込み、その中からランダムにカテゴリを選び、さらにそのカテゴリの商品一覧からランダムに商品を 1 件選んで表示するローカルビューアーです。

## データソース

- 元データ: `projects/monotaro-category-scraper/data/all-categories.deep.categories.csv`
- viewer 用コピー先: `projects/webpage-viewer/backend/data/monotaro_categories.deep.csv`
- JSON も保管: `projects/webpage-viewer/backend/data/monotaro_categories.deep.json`

viewer はコピー先の CSV を読み込み、`is_leaf=True` かつ `product_count > 0` のカテゴリだけを抽出対象にします。

## 動作

1. コピー済みカテゴリ CSV からランダムに最下層カテゴリを選ぶ
2. そのカテゴリの一覧ページ番号をランダムに決める
3. 一覧ページ内の商品をランダムに 1 件選ぶ
4. 商品ページからメイン画像を取得して表示する

## 起動

```bash
cd "/Users/kentaro/Library/CloudStorage/Dropbox-NoRA/nomura kentaro/_Scripts/_app/web-scraping-apps/projects/webpage-viewer"
python3 backend/server.py
```

ブラウザで `http://127.0.0.1:8011` を開きます。

## Render

このプロジェクトには [render.yaml](/Users/kentaro/Library/CloudStorage/Dropbox-NoRA/nomura%20kentaro/_Scripts/_app/web-scraping-apps/projects/webpage-viewer/render.yaml) を入れてあります。Render 側では次の設定で起動します。

- `Root Directory`: `web-scraping-apps/projects/webpage-viewer`
- `Build Command`: `pip install -r requirements.txt`
- `Start Command`: `python3 backend/server.py`

Blueprint を使わず手動で作る場合も、この 3 つをそのまま入れれば動きます。

## 画面の入力項目

- `Category Data File`
  - 読み込み元のコピー済み CSV 表示用です
- `Random Products / Load`
  - 1 回のロードで集める商品数です
- `Cookie Header (optional)`
  - MonotaRO 側で必要な場合だけ入れます
- `Delay (ms)`
  - MonotaRO へのアクセス間隔です

## カテゴリデータを更新したいとき

```bash
cp "projects/monotaro-category-scraper/data/all-categories.deep.categories.csv" "projects/webpage-viewer/backend/data/monotaro_categories.deep.csv"
cp "projects/monotaro-category-scraper/data/all-categories.deep.categories.json" "projects/webpage-viewer/backend/data/monotaro_categories.deep.json"
```

## 注意

- MonotaRO へのアクセスに失敗したカテゴリはスキップして次を試します
- 表示している画像の右クリックで商品ページを開けます
- ランダム取得なので、同じカテゴリや商品が再度選ばれることはあります

# Scrapbox Daily Input Images

Scrapbox の URL を入力し、指定タグ付きページから画像だけを抽出して表示するローカルブラウザアプリです。

## できること

- Scrapbox の URL から project 名を自動抽出
- `#dailyinput` が付いたページを走査
- ページ内の画像 URL を抽出してギャラリー表示
- public project に加えて、`connect.sid` を使って private project の取得にも対応

## 起動

```bash
cd "c:\Users\lapla\NoRA Dropbox\nomura kentaro\_Scripts\_app\web-scraping-apps\projects\scrapbox-viewer"
python backend\server.py
```

ブラウザで `http://127.0.0.1:8011` を開きます。

## 入力項目

- `Scrapbox URL`
  - 例: `https://scrapbox.io/my-project`
- `Tag`
  - デフォルトは `#dailyinput`
- `connect.sid`
  - private project の場合のみ指定

## 注意

- 画像抽出は各ページ本文を走査して行うため、ページ数が多い project では時間がかかります。
- `connect.sid` は保存していません。各 API リクエスト時にそのまま Scrapbox へ転送します。
- 抽出対象は、ページ本文中の画像 URL と Scrapbox API が返す画像フィールドです。

## Render で公開する

このプロジェクトには [render.yaml](/c:/Users/lapla/NoRA Dropbox/nomura kentaro/_Scripts/_app/web-scraping-apps/projects/scrapbox-viewer/render.yaml) を入れてあります。

手順:

1. このコードを GitHub に push する
2. Render で `New +` -> `Blueprint` を選ぶ
3. GitHub リポジトリを接続する
4. `scrapbox-viewer` サービスを作成する
5. デプロイ完了後に発行された URL を開く

補足:

- `backend/server.py` は `PORT` 環境変数を読むので Render 上でそのまま起動できます
- private Scrapbox の `connect.sid` を公開サーバーに入れる運用は避けるほうが安全です
- 公開版は public project 用と考えるのが無難です

# Reschene E2E テスト手順

アップロードから 3D 再構築コンテナ起動までの全フローを通すための手順。

## 前提条件

- AWS CLI がインストール・ログイン済み
- `uv` がインストール済み
- CDK スタックがデプロイ済み (`npx cdk deploy`)
- Cognito テストユーザーが作成済み

```
API_ENDPOINT=https://gylct49hy6.execute-api.us-east-1.amazonaws.com
USER_POOL_ID=us-east-1_CEAxv1pIt
CLIENT_ID=7155g9onuvp3tu50hs2n1dpufl
USERNAME=testuser@reschene.example.com
PASSWORD=TestPass123!
```

---

## 1. 既存データのクリーンアップ

3 つのバケットを空にする。

```bash
aws s3 rm s3://reschene-userimage/ --recursive
aws s3 rm s3://reschene-metadata/ --recursive
aws s3 rm s3://reschene-thumbnails/ --recursive
```

3D output バケットのロックファイルも削除（前回テストの残留防止）:

```bash
aws s3 rm s3://reschene-3d-output/ --recursive
```

空になったことを確認:

```bash
aws s3 ls s3://reschene-userimage/ --recursive --summarize | grep "Total Objects"
aws s3 ls s3://reschene-metadata/ --recursive --summarize | grep "Total Objects"
aws s3 ls s3://reschene-thumbnails/ --recursive --summarize | grep "Total Objects"
```

---

## 2. Cognito トークン取得

```bash
TOKEN=$(aws cognito-idp initiate-auth \
  --auth-flow USER_PASSWORD_AUTH \
  --client-id 7155g9onuvp3tu50hs2n1dpufl \
  --auth-parameters USERNAME=testuser@reschene.example.com,PASSWORD=TestPass123! \
  --region us-east-1 \
  --query 'AuthenticationResult.IdToken' \
  --output text)
echo "Token length: ${#TOKEN}"
```

> トークンの有効期限は 1 時間。以降のステップで期限切れになった場合は再取得する。

---

## 3. Presigned URL 取得テスト

少数ファイルで API の基本動作を確認:

```bash
curl -s -X POST "$API_ENDPOINT/upload/presigned-url" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"files": [{"filename": "test1.jpg"}, {"filename": "test2.jpg"}]}' \
  | python3 -m json.tool
```

期待結果:
- HTTP 200
- `upload_id` (UUID) が返る
- `urls` 配列に 2 件の presigned URL が含まれる

---

## 4. 98 枚一括アップロード

`tests/bulk_upload.py` を使用（20 並列）:

```bash
uv run python tests/bulk_upload.py
```

期待結果:
- 98 枚全て成功 (`98 ok / 0 fail`)
- 所要時間 15 秒前後
- `Upload ID` が表示される

> **upload_id を控えておく** -- 後続のバッチ検索テストで使用する。

---

## 5. メタデータ・サムネイル生成の確認

アップロード後、Lambda が非同期処理を行う。**約 45 秒待ってから**確認:

```bash
sleep 45

# メタデータ (raw/ 配下に個別 JSON ファイル)
aws s3 ls s3://reschene-metadata/raw/ --recursive --summarize | grep "Total Objects"

# サムネイル
aws s3 ls s3://reschene-thumbnails/ --recursive --summarize | grep "Total Objects"
```

期待結果:
- メタデータ: `Total Objects: 98`
- サムネイル: `Total Objects: 98`

メタデータの中身を 1 件確認:

```bash
aws s3 ls s3://reschene-metadata/raw/ --recursive | head -1 | awk '{print $4}' | \
  xargs -I{} aws s3 cp s3://reschene-metadata/{} - | python3 -m json.tool
```

期待されるフィールド: `user_id`, `s3_key`, `upload_id`, `original_filename`,
`file_size`, `uploaded_at`, `camera_make`, `camera_model`, `datetime_original`,
`gps_latitude`, `gps_longitude`, `gps_altitude`

---

## 6. 検索 API テスト

### 6a. user_images 検索

```bash
curl -s -X POST "$API_ENDPOINT/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "user_images"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Results: {len(d[\"results\"])}')"
```

期待結果: `Results: 98`

### 6b. batch 検索

```bash
UPLOAD_ID="<ステップ4で控えたupload_id>"

curl -s -X POST "$API_ENDPOINT/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"type\": \"batch\", \"upload_id\": \"$UPLOAD_ID\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Results: {len(d[\"results\"])}')"
```

期待結果: `Results: 98`

### 6c. geo_radius 検索

テスト画像の GPS 座標（東京都東久留米市付近）を中心に半径 1km で検索:

```bash
curl -s -X POST "$API_ENDPOINT/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "geo_radius", "latitude": 35.700, "longitude": 139.518, "radius_km": 1}' \
  | python3 -c "
import sys,json
d=json.load(sys.stdin)
r=d['results']
print(f'Results: {len(r)}')
if r: print(f'Nearest: {r[0][\"distance_km\"]} km')
"
```

期待結果: `Results: 98`, `Nearest: ~0.016 km`

---

## 7. 画像 URL 取得テスト

検索結果から `s3_key` を 1 件取得し、presigned GET URL を発行:

```bash
S3_KEY=$(curl -s -X POST "$API_ENDPOINT/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "user_images"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['results'][0]['s3_key'])")

curl -s -G "$API_ENDPOINT/images/url" \
  --data-urlencode "s3_key=$S3_KEY" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

期待結果:
- HTTP 200
- `presigned_url` が返る
- `expires_in: 86400` (24 時間)

返された `presigned_url` で実際にダウンロードできることを確認:

```bash
PRESIGNED_URL=$(curl -s -G "$API_ENDPOINT/images/url" \
  --data-urlencode "s3_key=$S3_KEY" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['presigned_url'])")

curl -s -o /dev/null -w "HTTP %{http_code}, Size: %{size_download} bytes\n" "$PRESIGNED_URL"
```

期待結果: `HTTP 200, Size: <数MB> bytes`

---

## 8. 3D 再構築コンテナの起動確認

### 起動条件

Reconstruction Judge Lambda は以下の条件で ECS タスクを起動する:

- GPS 付き画像が同一地域（半径 1km 以内）に **50 枚以上** 存在する
- 同一リージョンキーのロックファイルが存在しない（タスク重複防止）

98 枚の GPS 付き画像をアップロードすると、この条件を自動的に満たす。

### 確認方法

アップロードから数分後に以下を確認:

```bash
# 3D output バケットに status.json / model.json が生成されているか
aws s3 ls s3://reschene-3d-output/ --recursive
```

期待結果:
```
<timestamp> <size> <geohash>/model.json
<timestamp> <size> <geohash>/status.json
```

status.json の中身を確認:

```bash
# geohash は ls の結果から取得（例: xn75nf）
GEOHASH=$(aws s3 ls s3://reschene-3d-output/ | head -1 | awk '{print $2}' | tr -d '/')
aws s3 cp "s3://reschene-3d-output/$GEOHASH/status.json" - | python3 -m json.tool
```

期待結果:
```json
{
    "region_key": "xn75nf",
    "status": "COMPLETED",
    "phase": "done",
    "progress_pct": 100,
    "center_latitude": 35.699...,
    "center_longitude": 139.518...,
    "completed_at": "2026-...",
    "output_s3_prefix": "s3://reschene-3d-output/xn75nf/",
    "error_message": null
}
```

model.json（mock 出力）:

```bash
aws s3 cp "s3://reschene-3d-output/$GEOHASH/model.json" - | python3 -m json.tool
```

### ECS タスク履歴の確認

```bash
# 停止済みタスク一覧
aws ecs list-tasks --cluster reschene-reconstruction --desired-status STOPPED --region us-east-1

# タスクの詳細（最新 1 件）
TASK_ARN=$(aws ecs list-tasks --cluster reschene-reconstruction \
  --desired-status STOPPED --region us-east-1 \
  --query 'taskArns[0]' --output text)

aws ecs describe-tasks --cluster reschene-reconstruction \
  --tasks "$TASK_ARN" --region us-east-1 \
  --query 'tasks[0].{Status:lastStatus,StopCode:stopCode,CreatedAt:createdAt,StoppedAt:stoppedAt}'
```

期待結果: `Status: STOPPED`, `StopCode: EssentialContainerExited`（正常終了）

### ASG インスタンスの確認

```bash
aws autoscaling describe-auto-scaling-groups --region us-east-1 \
  --query 'AutoScalingGroups[?contains(AutoScalingGroupName, `Reschene`)].{Desired:DesiredCapacity,Instances:length(Instances)}' \
  --output table
```

> ECS Managed Scaling により、タスク需要に応じて g4dn.xlarge インスタンスが
> 0-2 台の範囲で自動スケールする。タスク完了後しばらくするとスケールインする。

---

## 9. コンパクション Lambda テスト

日次スケジュール（毎日 03:00 UTC）とは別に、手動で即時実行できる:

```bash
aws lambda invoke \
  --function-name reschene-compaction \
  --payload '{}' \
  --region us-east-1 \
  /tmp/compaction_result.json

cat /tmp/compaction_result.json | python3 -m json.tool
```

期待結果:
```json
{
    "statusCode": 200,
    "compacted": 98,
    "total": 98
}
```

コンパクション後の確認:

```bash
# raw/ は空になる
aws s3 ls s3://reschene-metadata/raw/ --recursive --summarize | grep "Total Objects"

# compacted/ に Parquet ファイルが生成される
aws s3 ls s3://reschene-metadata/compacted/
```

コンパクション後も検索 API が正常動作することを確認:

```bash
curl -s -X POST "$API_ENDPOINT/search" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "user_images"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Results: {len(d[\"results\"])}')"
```

期待結果: `Results: 98`（Parquet から読み取り）

---

## 10. 削除 + クリーンアップテスト

画像を 1 枚削除し、Cleanup Lambda が自動的にサムネイルとメタデータを削除することを確認:

```bash
# 削除対象の s3_key を取得
TARGET=$(aws s3 ls s3://reschene-userimage/ --recursive | head -1 | awk '{print $4}')
USER_ID=$(echo "$TARGET" | cut -d'/' -f1)
UPLOAD_ID=$(echo "$TARGET" | cut -d'/' -f2)
FILENAME=$(echo "$TARGET" | cut -d'/' -f3)

echo "Deleting: $TARGET"

# 削除前の確認
echo -n "Thumbnail exists: "
aws s3 ls "s3://reschene-thumbnails/$TARGET" | wc -l

# 画像を削除
aws s3 rm "s3://reschene-userimage/$TARGET"

# 15 秒待つ
sleep 15

# 削除後の確認
echo -n "Thumbnail exists: "
aws s3 ls "s3://reschene-thumbnails/$TARGET" | wc -l
```

期待結果:
- 削除前: `Thumbnail exists: 1`
- 削除後: `Thumbnail exists: 0`

---

## テスト結果サマリー

| # | テスト項目 | 期待結果 |
|---|-----------|---------|
| 1 | データクリーンアップ | 3 バケット全て空 |
| 2 | Cognito 認証 | ID トークン取得成功 |
| 3 | Presigned URL 取得 | HTTP 200, upload_id + URL 配列 |
| 4 | 98 枚アップロード | 98/98 成功, 15 秒以内 |
| 5 | メタデータ抽出 | raw/ に 98 個の JSON ファイル |
| 6 | サムネイル生成 | 98 個のサムネイル |
| 7 | 検索 user_images | 98 件返却 |
| 8 | 検索 batch | 98 件返却 |
| 9 | 検索 geo_radius | 98 件返却, 距離計算あり |
| 10 | 画像 URL 取得 | presigned GET URL, ダウンロード成功 |
| 11 | 3D 再構築コンテナ起動 | status.json に COMPLETED, model.json 生成 |
| 12 | コンパクション | raw → Parquet 変換, 検索引き続き正常 |
| 13 | 削除 + クリーンアップ | サムネイル・メタデータ自動削除 |

---

## トラブルシューティング

### トークン取得エラー: `MissingDependencyException`

`uv run` 経由で boto3 を使う場合に発生する場合がある。

```bash
uv add "botocore[crt]"
```

または `COGNITO_TOKEN` 環境変数で事前取得したトークンを渡す:

```bash
export COGNITO_TOKEN=$(aws cognito-idp initiate-auth ... --query 'AuthenticationResult.IdToken' --output text)
uv run python tests/bulk_upload.py
```

### 3D 再構築が発火しない

- 閾値を確認: `RECONSTRUCTION_THRESHOLD=50`（GPS 付き画像 50 枚以上が必要）
- ロックファイルの残留を確認:
  ```bash
  aws s3 ls s3://reschene-3d-output/ --recursive | grep lock.json
  ```
  残っていたら削除: `aws s3 rm s3://reschene-3d-output/<geohash>/lock.json`
- CloudWatch Logs で judge Lambda のログを確認:
  ```bash
  aws logs tail /aws/lambda/reschene-reconstruction-judge --since 30m
  ```

### コンパクション後に検索結果が 0 件

Glue テーブルが正しくデプロイされているか確認:

```bash
aws glue get-table --database-name reschene --name image_metadata_raw --region us-east-1 \
  --query 'Table.StorageDescriptor.Location'
aws glue get-table --database-name reschene --name image_metadata_compacted --region us-east-1 \
  --query 'Table.StorageDescriptor.Location'
```

期待: `s3://reschene-metadata/raw/` と `s3://reschene-metadata/compacted/`

### ASG インスタンスが残り続ける

ECS Managed Scaling のクールダウン時間（デフォルト 5 分程度）後にスケールインする。
即時スケールインしたい場合:

```bash
ASG_NAME=$(aws autoscaling describe-auto-scaling-groups --region us-east-1 \
  --query 'AutoScalingGroups[?contains(AutoScalingGroupName, `Reschene`)].AutoScalingGroupName' \
  --output text)
aws autoscaling set-desired-capacity --auto-scaling-group-name "$ASG_NAME" \
  --desired-capacity 0 --region us-east-1
```

package blob_downloader

import (
	"context"
	"fmt"
	"bytes"
	"encoding/json"
	"github.com/bluesky-social/indigo/atproto/identity"
    "github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/atproto/data"
    atprepo "github.com/bluesky-social/indigo/atproto/repo"
    "github.com/bluesky-social/indigo/repo"
    "os"
    "path/filepath"
    "github.com/bluesky-social/indigo/xrpc"
    "github.com/ipfs/go-cid"
     comatproto "github.com/bluesky-social/indigo/api/atproto"
	//  "github.com/bluesky-social/indigo/cmd/goat"
    "strings"
    _"github.com/bluesky-social/indigo/api/bsky" // 注册 Bluesky 的记录类型
    
)

// func main() {
// 	da, err := GetRepoRecordsAsMap("tammybskysocial.bsky.social")
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 	} else {
//         blobMaps := repoMap2blob(da)
//         for _, blobMap := range blobMaps {
//             fmt.Printf("%v\n", blobMap)
//         }
 
// 		}
// 	}


 // 获取repo的记录，返回map 形式
func GetRepoRecordsAsMap(target_atid string) (map[string]interface{}, error) {
    ctx := context.Background()
    atid, err := syntax.ParseAtIdentifier(target_atid)
    if err != nil {
        return nil, err
    }

    dir := identity.DefaultDirectory()
    ident, err := dir.Lookup(ctx, *atid)
    if err != nil {
        return nil, err
    }

    if ident.PDSEndpoint() == "" {
        return nil, fmt.Errorf("no PDS endpoint for identity")
    }

    // 获取 repo 的 .car 文件内容（字节流）
    xrpcc := xrpc.Client{
        Host: ident.PDSEndpoint(),
    }
    repoBytes, err := comatproto.SyncGetRepo(ctx, &xrpcc, ident.DID.String(), "")
    if err != nil {
        return nil, err
    }

    // 用内存流解析 .car 文件
    fi := bytes.NewReader(repoBytes)
    _, r, err := atprepo.LoadRepoFromCAR(ctx, fi)
    if err != nil {
        return nil, fmt.Errorf("failed to read repo from CAR: %v", err)
    }

    result := make(map[string]interface{})

    // 遍历 MST，获取所有记录
    err = r.MST.Walk(func(k []byte, v cid.Cid) error {
        col, rkey, err := syntax.ParseRepoPath(string(k))
        if err != nil {
            return err
        }
        recBytes, _, err := r.GetRecordBytes(ctx, col, rkey)
        if err != nil {
            return err
        }
        rec, err := data.UnmarshalCBOR(recBytes)
        if err != nil {
            return err
        }
        // 你可以直接存 rec，也可以存 JSON 反序列化后的对象
        result[string(k)] = rec
        return nil
    })
    if err != nil {
        return nil, err
    }

    // 也可以把 commit 信息加进去
    // commitJSON, _ := json.Marshal(c)
    // result["_commit"] = commitJSON

    return result, nil
}

// 从全量的repo记录中找到对应的图片类型及其blob，返回一个map的list，其中包含图片的ref，size，mimetype
func repoMap2blob(repoMap map[string]interface{})[]map[string]interface{} {
    var allBlobMaps []map[string]interface{}

    for _, value := range repoMap {
        // Convert value to JSON string to check for blob/mimetype
        jsonStr, err := json.Marshal(value)
        if err != nil {
            fmt.Printf("Error marshaling value: %v\n", err)
            continue
        }
        // Check if JSON string contains blob or mimetype
        jsonString := string(jsonStr)
        if strings.Contains(strings.ToLower(jsonString), "blob") || 
           strings.Contains(strings.ToLower(jsonString), "mimetype") {
            fmt.Printf("JSON string: %v\n", jsonString)
            // 检查 value 的类型并提取 key
            fmt.Printf("Type of value: %T\n", value)
            // 创建一个集合存储所有image值
            imageSet := make(map[interface{}]bool)            
            
            
            // 开始递归查找
            findImages(value, imageSet)
            
            // 打印所有找到的image值
            for img := range imageSet {
                if blobVal, ok := img.(data.Blob); ok {
                    blobMap, err := BlobToMap(blobVal)
                    if err == nil {
                        allBlobMaps = append(allBlobMaps, blobMap)
                    }
                }
            }
    }
    }
    return allBlobMaps

}

// 递归查找map中的image
func findImages(v interface{}, imageSet map[interface{}]bool) {
    switch val := v.(type) {
    case map[string]interface{}:
        // 检查当前map是否包含image键
        if img, ok := val["image"]; ok {

            imageSet[img] = true
            fmt.Printf("Found image: %v\n", img)
        }
        // 递归检查所有值
        for _, subVal := range val {
            findImages(subVal, imageSet)
        }
    case map[interface{}]interface{}:
        // 处理interface{}类型的map
        if img, ok := val["image"]; ok {
            imageSet[img] = true
            fmt.Printf("Found image: %v\n", img)
        }
        // 递归检查所有值
        for _, subVal := range val {
            findImages(subVal, imageSet)
        }
    case []interface{}:
        // 递归检查数组中的每个元素
        for _, item := range val {
            findImages(item, imageSet)
        }
    }
}


func BlobToMap(blob data.Blob) (map[string]interface{}, error) {
    var m map[string]interface{}
    b, err := json.Marshal(blob)
    if err != nil {
        return nil, err
    }
    err = json.Unmarshal(b, &m)
    if err != nil {
        return nil, err
    }
    return m, nil
}


// 处理单个 .car 文件
func processCarFile(carPath string) error {
    ctx := context.Background()
    // 打开文件
    fi, err := os.Open(carPath)
    if err != nil {
        return fmt.Errorf("failed to open file: %v", err)
    }
    defer fi.Close()
    
    // 读取仓库数据
    r, err := repo.ReadRepoFromCar(ctx, fi)
    if err != nil {
        return fmt.Errorf("failed to read repo from CAR: %v", err)
    }
    // extract DID from repo commit
    sc := r.SignedCommit()
    did, err := syntax.ParseDID(sc.Did)
    if err != nil {
        return err
    }
    topDir := did.String()
    fmt.Println("topDir: ", topDir)
    // iterate over all of the records by key and CID
    err = r.ForEach(ctx, "", func(k string, v cid.Cid) error {
        recPath := topDir + "/" + k
    os.MkdirAll(filepath.Dir(recPath), os.ModePerm)
    if err != nil {
        return err
    }
    // fetch the record CBOR and convert to a golang struct
    fmt.Println("k:",k)
    _, rec, err := r.GetRecord(ctx, k)
    if err != nil {
        return err
    }
    // serialize as JSON
    recJson, err := json.MarshalIndent(rec, "", "  ")
    if err != nil {
        return err
    }
    if err := os.WriteFile(recPath+".json", recJson, 0666); err != nil {
        return err
    }
            // fmt.Printf("%s\t%s\n", k, v.String())
            return nil
        })
        if err != nil {
            return err
        }
    return nil
}



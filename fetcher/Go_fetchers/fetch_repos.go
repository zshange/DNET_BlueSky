package main
import (
    "encoding/json"
    "context"
    "fmt"
	"github.com/bluesky-social/indigo/atproto/identity"
    "github.com/bluesky-social/indigo/atproto/syntax"
    "os"
    "path/filepath"
    "github.com/bluesky-social/indigo/xrpc"
    "github.com/ipfs/go-cid"
     comatproto "github.com/bluesky-social/indigo/api/atproto"
     "github.com/bluesky-social/indigo/repo"
    "strings"
    _"github.com/bluesky-social/indigo/api/bsky" // 注册 Bluesky 的记录类型
    // "github.com/bluesky-social/indigo/atproto/data" // 用于类型无关的数据处理
	// "github.com/bluesky-social/indigo/automod"
    )
func main() {
    if err := get_repos("shange.bsky.social"); err != nil {
        fmt.Println("Error:", err)
    } else {
        // 处理所有 .car 文件
        processAllCarFiles("../../repo_files")
    }
    if err := fetch_metadata("shange.bsky.social"); err != nil {
        fmt.Println("Error:", err)
    } else {
        // 处理所有 .car 文件
        // processAllCarFiles("../../repo_files")
    }
}

func fetch_metadata(target_atid string) error {
    ctx := context.Background()

    // 替换为你想查询的标识符，比如 ""
    atid, err := syntax.ParseAtIdentifier(target_atid)
    if err != nil {
        return err
    }

    dir := identity.DefaultDirectory()
    ident, err := dir.Lookup(ctx, *atid)
    if err != nil {
        return err
    }

    if ident.PDSEndpoint() == "" {
        return fmt.Errorf("no PDS endpoint for identity")
    }

    fmt.Println("PDS Endpoint:", ident.PDSEndpoint())

    // // 获取用户信息，将文件存储在repo_files目录下
    storageDir := "../../repo_files"
    if err := os.MkdirAll(storageDir, 0755); err != nil {
        return fmt.Errorf("failed to create storage directory: %v", err)
    }

    
    // carPath := filepath.Join(storageDir, ident.DID.String() + ".car")

    xrpcc := xrpc.Client{
        Host: ident.PDSEndpoint(),
    }
    info,err := comatproto.AdminGetAccountInfo(ctx, &xrpcc, ident.DID.String())
    if err != nil {
        return err
    }
    fmt.Println(info)
    return nil
}

// 处理目录中所有的 .car 文件
func processAllCarFiles(dirPath string) {
    // 获取所有 .car 文件
    carFiles, err := scanCarFiles(dirPath)
    if err != nil {
        fmt.Printf("Error scanning directory: %v\n", err)
        return
    }
    if len(carFiles) == 0 {
        fmt.Println("No .car files found")
        return
    }
    fmt.Printf("Found %d .car files:\n", len(carFiles))

    // 遍历每个 .car 文件
    for i, carPath := range carFiles {
        fmt.Printf("%d. Processing file: %s\n", i+1, carPath)
        
        // 处理每个文件
        err := processCarFile(carPath)
        if err != nil {
            fmt.Printf("Error processing %s: %v\n", carPath, err)
            continue
        }
    }
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

// 扫描目录，找出所有 .car 文件
func scanCarFiles(dirPath string) ([]string, error) {
    var carFiles []string
    // 检查目录是否存在
    _, err := os.Stat(dirPath)
    if os.IsNotExist(err) {
        return nil, fmt.Errorf("directory does not exist: %s", dirPath)
    }
    // 遍历目录中的所有文件
    err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        // 跳过目录，只处理文件
        if info.IsDir() {
            return nil
        }
        // 检查文件是否以 .car 结尾
        if strings.HasSuffix(info.Name(), ".car") {
            carFiles = append(carFiles, path)
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    return carFiles, nil
}

// 通过用户标识符获取用户仓库
func get_repos(target_atid string) error {
    ctx := context.Background()

    // 替换为你想查询的标识符，比如 ""
    atid, err := syntax.ParseAtIdentifier(target_atid)
    if err != nil {
        return err
    }

    dir := identity.DefaultDirectory()
    ident, err := dir.Lookup(ctx, *atid)
    if err != nil {
        return err
    }

    if ident.PDSEndpoint() == "" {
        return fmt.Errorf("no PDS endpoint for identity")
    }

    fmt.Println("PDS Endpoint:", ident.PDSEndpoint())

    // // 获取用户信息，将文件存储在repo_files目录下
    storageDir := "../../repo_files"
    if err := os.MkdirAll(storageDir, 0755); err != nil {
        return fmt.Errorf("failed to create storage directory: %v", err)
    }

    
    carPath := filepath.Join(storageDir, ident.DID.String() + ".car")

    xrpcc := xrpc.Client{
        Host: ident.PDSEndpoint(),
    }
    repoBytes, err := comatproto.SyncGetRepo(ctx, &xrpcc, ident.DID.String(), "")
    if err != nil {
        return err
    }

    // 写入文件
    err = os.WriteFile(carPath, repoBytes, 0666)
    if err != nil {
        return err
    }
    fmt.Printf("CAR file saved to: %s\n", carPath)
    return nil
}

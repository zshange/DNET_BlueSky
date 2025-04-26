package main
import (
    "context"
    "fmt"
	"github.com/bluesky-social/indigo/atproto/identity"
    "github.com/bluesky-social/indigo/atproto/syntax"
    "os"
    "path/filepath"
    "github.com/bluesky-social/indigo/xrpc"
     comatproto "github.com/bluesky-social/indigo/api/atproto"
)
func main() {
    if err := get_pds_endpoint("shange.bsky.social"); err != nil {
        fmt.Println("Error:", err)
    }
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

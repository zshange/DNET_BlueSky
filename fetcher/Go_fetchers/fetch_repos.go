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
    "reflect"
    _"github.com/bluesky-social/indigo/api/bsky" // 注册 Bluesky 的记录类型
    // "github.com/bluesky-social/indigo/atproto/data" // 用于类型无关的数据处理
	// "github.com/bluesky-social/indigo/automod"
	"github.com/bluesky-social/indigo/api/bsky"
	// "github.com/bluesky-social/indigo/util"
)

func getFollowsCount(ctx context.Context, c *xrpc.Client, actor string) (int64, error) {
	resp, err := bsky.ActorGetProfile(ctx, c, actor)
	if err != nil {
		return 0, err
	}
	if resp.FollowsCount == nil {
		return 0, fmt.Errorf("no followsCount in profile")
	}
	return *resp.FollowsCount, nil
}

func getFollowersCount(ctx context.Context, c *xrpc.Client, actor string) (int64, error) {
	p, err := bsky.ActorGetProfile(ctx, c, actor)
	if err != nil {
		return 0, err
	}
	if p.FollowersCount == nil {
		return 0, fmt.Errorf("no followersCount in profile")
	}
	return *p.FollowersCount, nil
}

func getAuthenticatedClient(ctx context.Context) (*xrpc.Client, error) {
    // 从环境变量获取认证信息（更安全的方式）
    handle := os.Getenv("BSKY_HANDLE")
    password := os.Getenv("BSKY_PASSWORD")
    
    // 如果环境变量为空，使用默认值（仅用于测试）
    if handle == "" {
        handle = "shange.bsky.social"
    }
    if password == "" {
        password = "zqj20030403"
    }
    
    // 创建临时客户端进行认证
    tempClient := &xrpc.Client{
        Host: "https://bsky.social",
    }
    // 获取会话
    session, err := comatproto.ServerCreateSession(ctx, tempClient, &comatproto.ServerCreateSession_Input{
        Identifier: handle,
        Password:   password,
    })
    if err != nil {
        return nil, fmt.Errorf("认证失败: %w", err)
    }
    
    // 创建认证客户端
    return &xrpc.Client{
        Host: "https://bsky.social",
        Auth: &xrpc.AuthInfo{
            AccessJwt: session.AccessJwt,
            Handle:    session.Handle,
            Did:       session.Did,
        },
    }, nil
}
func main() {
    ctx := context.Background()
    // if err := get_repos("kyopink.bsky.social"); err != nil {
    //     fmt.Println("Error:", err)
    // } else {
    //     // 处理所有 .car 文件
    //     processAllCarFiles("../../repo_files")
    // }

    c, err := getAuthenticatedClient(ctx)
    if err != nil {
        fmt.Println("xrpc client error:", err)
        return
    }
    
    // 获取用户profile
    profile, err := FetchProfile(ctx, c, "kyopink.bsky.social")
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    
    // 使用新的函数来格式化输出profile信息
    printProfile(profile)
    
    // 显示profile包含的所有字段
    printProfileFields(profile)
    printProfileFieldsReflection(profile) 
    
    // 如果需要查看完整的JSON数据，取消下面的注释
    // printProfileAsJSON(profile)
    
    // // 也可以单独获取关注数和粉丝数
    // followsCount, err := getFollowsCount(ctx, c, "kyopink.bsky.social")
    // if err != nil {
    //     fmt.Println("获取关注数错误:", err)
    // } else {
    //     fmt.Printf("关注数: %d\n", followsCount)
    // }
    
    // followersCount, err := getFollowersCount(ctx, c, "kyopink.bsky.social")
    // if err != nil {
    //     fmt.Println("获取粉丝数错误:", err)
    // } else {
    //     fmt.Printf("粉丝数: %d\n", followersCount)
    // }
}

func FetchProfile(ctx context.Context, cli *xrpc.Client, actor string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	// 调 app.bsky.actor.getProfile，返回 ProfileViewDetailed 结构
	return bsky.ActorGetProfile(ctx, cli, actor)
}

// 添加一个函数来格式化输出profile信息
func printProfile(profile *bsky.ActorDefs_ProfileViewDetailed) {
    fmt.Println("=== 用户Profile信息 ===")
    fmt.Printf("DID: %s\n", profile.Did)
    fmt.Printf("Handle: %s\n", profile.Handle)
    
    if profile.DisplayName != nil {
        fmt.Printf("显示名: %s\n", *profile.DisplayName)
    }
    
    if profile.Description != nil {
        fmt.Printf("简介: %s\n", *profile.Description)
    }
    
    if profile.FollowersCount != nil {
        fmt.Printf("粉丝数: %d\n", *profile.FollowersCount)
    }
    
    if profile.FollowsCount != nil {
        fmt.Printf("关注数: %d\n", *profile.FollowsCount)
    }
    
    if profile.PostsCount != nil {
        fmt.Printf("帖子数: %d\n", *profile.PostsCount)
    }
    
    if profile.Avatar != nil {
        fmt.Printf("头像: %s\n", *profile.Avatar)
    }
    
    if profile.Banner != nil {
        fmt.Printf("横幅: %s\n", *profile.Banner)
    }
    
    if profile.CreatedAt != nil {
        fmt.Printf("创建时间: %s\n", *profile.CreatedAt)
    }
    
    fmt.Println("========================")
}

// 添加一个函数来以JSON格式输出profile信息（方便调试）
func printProfileAsJSON(profile *bsky.ActorDefs_ProfileViewDetailed) {
    fmt.Println("=== Profile JSON ===")
    profileJSON, err := json.MarshalIndent(profile, "", "  ")
    if err != nil {
        fmt.Printf("JSON序列化错误: %v\n", err)
        return
    }
    fmt.Println(string(profileJSON))
    fmt.Println("==================")
}

// 添加一个函数来显示profile结构体的所有字段名
func printProfileFields(profile *bsky.ActorDefs_ProfileViewDetailed) {
    fmt.Println("=== Profile包含的所有字段 ===")
    // 基本字段
    fmt.Printf("%-20s: %s\n", "Did", profile.Did)
    fmt.Printf("%-20s: %s\n", "Handle", profile.Handle)
    // 可选字段（使用指针）
    if profile.DisplayName != nil {
        fmt.Printf("%-20s: %s\n", "DisplayName", *profile.DisplayName)
    } else {
        fmt.Printf("%-20s: %s\n", "DisplayName", "nil")
    }
    if profile.Description != nil {
        fmt.Printf("%-20s: %s\n", "Description", *profile.Description)
    } else {
        fmt.Printf("%-20s: %s\n", "Description", "nil")
    }
    
    if profile.Avatar != nil {
        fmt.Printf("%-20s: %s\n", "Avatar", *profile.Avatar)
    } else {
        fmt.Printf("%-20s: %s\n", "Avatar", "nil")
    }
    
    if profile.Banner != nil {
        fmt.Printf("%-20s: %s\n", "Banner", *profile.Banner)
    } else {
        fmt.Printf("%-20s: %s\n", "Banner", "nil")
    }
    
    if profile.FollowersCount != nil {
        fmt.Printf("%-20s: %d\n", "FollowersCount", *profile.FollowersCount)
    } else {
        fmt.Printf("%-20s: %s\n", "FollowersCount", "nil")
    }
    
    if profile.FollowsCount != nil {
        fmt.Printf("%-20s: %d\n", "FollowsCount", *profile.FollowsCount)
    } else {
        fmt.Printf("%-20s: %s\n", "FollowsCount", "nil")
    }
    
    if profile.PostsCount != nil {
        fmt.Printf("%-20s: %d\n", "PostsCount", *profile.PostsCount)
    } else {
        fmt.Printf("%-20s: %s\n", "PostsCount", "nil")
    }
    
    if profile.CreatedAt != nil {
        fmt.Printf("%-20s: %s\n", "CreatedAt", *profile.CreatedAt)
    } else {
        fmt.Printf("%-20s: %s\n", "CreatedAt", "nil")
    }
    
    if profile.IndexedAt != nil {
        fmt.Printf("%-20s: %s\n", "IndexedAt", *profile.IndexedAt)
    } else {
        fmt.Printf("%-20s: %s\n", "IndexedAt", "nil")
    }
    
    // Labels 字段 (数组)
    if profile.Labels != nil && len(profile.Labels) > 0 {
        fmt.Printf("%-20s: %d个标签\n", "Labels", len(profile.Labels))
        for i, label := range profile.Labels {
            fmt.Printf("  Label[%d]: %+v\n", i, label)
        }
    } else {
        fmt.Printf("%-20s: %s\n", "Labels", "nil或空")
    }
    
    // Viewer 字段（观察者视角信息）
    if profile.Viewer != nil {
        fmt.Printf("%-20s: 存在\n", "Viewer")
        if profile.Viewer.Muted != nil {
            fmt.Printf("  %-18s: %t\n", "Muted", *profile.Viewer.Muted)
        }
        if profile.Viewer.BlockedBy != nil {
            fmt.Printf("  %-18s: %t\n", "BlockedBy", *profile.Viewer.BlockedBy)
        }
        if profile.Viewer.Blocking != nil {
            fmt.Printf("  %-18s: %s\n", "Blocking", *profile.Viewer.Blocking)
        }
        if profile.Viewer.Following != nil {
            fmt.Printf("  %-18s: %s\n", "Following", *profile.Viewer.Following)
        }
        if profile.Viewer.FollowedBy != nil {
            fmt.Printf("  %-18s: %s\n", "FollowedBy", *profile.Viewer.FollowedBy)
        }
    } else {
        fmt.Printf("%-20s: %s\n", "Viewer", "nil")
    }
    
    // PinnedPost 字段
    if profile.PinnedPost != nil {
        fmt.Printf("%-20s: 存在\n", "PinnedPost")
        fmt.Printf("  %-18s: %s\n", "CID", profile.PinnedPost.Cid)
        fmt.Printf("  %-18s: %s\n", "URI", profile.PinnedPost.Uri)
    } else {
        fmt.Printf("%-20s: %s\n", "PinnedPost", "nil")
    }
    
    fmt.Println("============================")
}

// 使用反射动态获取profile结构体的所有字段名和类型
func printProfileFieldsReflection(profile *bsky.ActorDefs_ProfileViewDetailed) {
    fmt.Println("=== Profile字段信息（反射版） ===")
    
    v := reflect.ValueOf(profile).Elem() // 获取指针指向的值
    t := reflect.TypeOf(profile).Elem()  // 获取指针指向的类型
    
    for i := 0; i < v.NumField(); i++ {
        field := t.Field(i)
        value := v.Field(i)
        
        fmt.Printf("%-20s: %-15s", field.Name, field.Type.String())
        
        // 如果字段是可导出的（公开的），尝试获取值
        if value.CanInterface() {
            switch value.Kind() {
            case reflect.Ptr:
                if !value.IsNil() {
                    fmt.Printf(" = %v", value.Elem().Interface())
                } else {
                    fmt.Printf(" = nil")
                }
            case reflect.Slice:
                if !value.IsNil() {
                    fmt.Printf(" = [%d元素]", value.Len())
                } else {
                    fmt.Printf(" = nil")
                }
            case reflect.String:
                fmt.Printf(" = \"%s\"", value.String())
            default:
                fmt.Printf(" = %v", value.Interface())
            }
        }
        fmt.Println()
    }
    fmt.Println("================================")
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

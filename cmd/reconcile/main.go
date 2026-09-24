// EXP-18：跨组件对账器。
//
// 三种模式：
//
//	reconcile -mode snapshot                      采集当前水位快照（JSON，含每 SKU 订单数/库存、全局计数、GTID）
//	reconcile -mode check [-baseline b.json]     对账：计数、集合一致性（orders↔outbox↔inbox↔notif）、
//	                                             库存守恒差分（需 -baseline）、账本逐单核验（需 -ledger）
//	reconcile -mode replay-scope                 输出恢复后需要重放的事件范围（PENDING + SENT 未消费）
//
// 账本格式（tests/load/fixed-rate.js LEDGER=1 产出，管道分隔）：
//
//	LEDGER|<order_id>|<user_id>|<sku>|<idem_key>|<epoch_ms>
//
// 退出码：0=通过；1=存在违约（集合违约 / 守恒破坏 / 账本 mismatch；账本 lost
// 仅在 -strict-ledger 时计为失败——全量备份档恢复 lost 是预期 RPO 结果，不是对账失败）。
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type skuWater struct {
	Orders int64 `json:"orders"`
	Stock  int64 `json:"stock"`
}

type snapshot struct {
	TS           string              `json:"ts"`
	GTIDExecuted string              `json:"gtid_executed,omitempty"`
	Counts       map[string]int64    `json:"counts"`
	SKU          map[string]skuWater `json:"sku"`
}

func main() {
	dsn := flag.String("dsn", os.Getenv("DB_DSN"), "MySQL DSN（默认取 DB_DSN 环境变量）")
	mode := flag.String("mode", "check", "snapshot | check | replay-scope")
	baseline := flag.String("baseline", "", "snapshot 模式产出的基线 JSON（守恒差分）")
	ledger := flag.String("ledger", "", "客户端成功响应账本文件（LEDGER|... 行）")
	strictLedger := flag.Bool("strict-ledger", false, "账本核验要求 lost==0（PITR 档）；默认 lost 仅报告")
	lostFile := flag.String("lost-file", "", "丢失订单清单输出文件（默认 stdout 摘要）")
	verbose := flag.Bool("v", false, "输出违约明细样本")
	flag.Parse()

	if *dsn == "" {
		fatal("需要 -dsn 或 DB_DSN 环境变量")
	}
	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		fatal("打开数据库失败: %v", err)
	}
	db.SetMaxOpenConns(4)
	if err := db.Ping(); err != nil {
		fatal("连接数据库失败: %v", err)
	}
	defer db.Close()

	switch *mode {
	case "snapshot":
		runSnapshot(db)
	case "check":
		runCheck(db, *baseline, *ledger, *strictLedger, *lostFile, *verbose)
	case "replay-scope":
		runReplayScope(db)
	default:
		fatal("未知模式 %q", *mode)
	}
}

func runSnapshot(db *sql.DB) {
	snap := snapshot{TS: time.Now().UTC().Format(time.RFC3339), Counts: map[string]int64{}, SKU: map[string]skuWater{}}

	var gtid sql.NullString
	_ = db.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtid)
	snap.GTIDExecuted = gtid.String

	counts, err := globalCounts(db)
	if err != nil {
		fatal("计数失败: %v", err)
	}
	snap.Counts = counts

	sku, err := currentSKUWater(db)
	if err != nil {
		fatal("SKU 水位失败: %v", err)
	}
	snap.SKU = sku

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&snap); err != nil {
		fatal("输出快照失败: %v", err)
	}
}

func globalCounts(db *sql.DB) (map[string]int64, error) {
	one := func(q string) (int64, error) {
		var n int64
		if err := db.QueryRow(q).Scan(&n); err != nil {
			return -1, err
		}
		return n, nil
	}
	m := map[string]int64{}
	queries := map[string]string{
		"orders":         `SELECT COUNT(*) FROM orders`,
		"outbox_sent":    `SELECT COUNT(*) FROM outbox_events WHERE status='SENT'`,
		"outbox_pending": `SELECT COUNT(*) FROM outbox_events WHERE status='PENDING'`,
		"outbox_dead":    `SELECT COUNT(*) FROM outbox_events WHERE status='DEAD'`,
		"inbox":          `SELECT COUNT(*) FROM inbox_events`,
		"notif":          `SELECT COUNT(*) FROM order_notifications`,
		"idem_total":     `SELECT COUNT(*) FROM idempotency`,
		"idem_orphan":    `SELECT COUNT(*) FROM idempotency WHERE order_id=0`,
	}
	for k, q := range queries {
		n, err := one(q)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		m[k] = n
	}
	return m, nil
}

type ledgerEntry struct {
	orderID int64
	user    string
	sku     string
	idem    string
	tsMs    int64
}

func loadLedger(path string) []ledgerEntry {
	f, err := os.Open(path)
	if err != nil {
		fatal("打开账本失败: %v", err)
	}
	defer f.Close()
	var out []ledgerEntry
	seen := map[int64]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "LEDGER|") {
			continue
		}
		p := strings.Split(line, "|")
		if len(p) != 6 {
			continue
		}
		id, err := strconv.ParseInt(p[1], 10, 64)
		if err != nil {
			continue
		}
		if seen[id] { // 幂等重放（200）与首发（201）指向同一订单，按 order_id 去重
			continue
		}
		seen[id] = true
		ts, _ := strconv.ParseInt(p[5], 10, 64)
		out = append(out, ledgerEntry{orderID: id, user: p[2], sku: p[3], idem: p[4], tsMs: ts})
	}
	return out
}

func runReplayScope(db *sql.DB) {
	var pending, sentNotInbox int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE status='PENDING'`).Scan(&pending); err != nil {
		fatal("%v", err)
	}
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM outbox_events ob
		WHERE ob.status='SENT'
		  AND NOT EXISTS (SELECT 1 FROM inbox_events ib WHERE ib.event_id = ob.event_id)`).Scan(&sentNotInbox); err != nil {
		fatal("%v", err)
	}
	fmt.Printf("重放范围（恢复数据内）：PENDING=%d + SENT未消费=%d = %d\n", pending, sentNotInbox, pending+sentNotInbox)
	fmt.Println("重置 SQL：")
	fmt.Println("  UPDATE outbox_events SET status='PENDING', attempts=0, sent_at=NULL")
	fmt.Println("   WHERE status='PENDING' OR (status='SENT' AND NOT EXISTS")
	fmt.Println("     (SELECT 1 FROM inbox_events ib WHERE ib.event_id = outbox_events.event_id));")
}

type setCheck struct {
	name  string
	query string
	bad   bool // 非 0 即违约
	note  string
}

func runCheck(db *sql.DB, baselinePath, ledgerPath string, strict bool, lostFile string, verbose bool) {
	fail := 0
	counts, err := globalCounts(db)
	if err != nil {
		fatal("计数失败: %v", err)
	}

	fmt.Println("== 全局计数 ==")
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-16s %d\n", k, counts[k])
	}

	// 集合一致性（正确性判据：违约必须为 0；非 bad 项是待办/证据而非违约）
	fmt.Println("== 集合一致性 ==")
	checks := []setCheck{
		{"orders 无 Outbox 事件", `SELECT COUNT(*) FROM orders o
			WHERE NOT EXISTS (SELECT 1 FROM outbox_events ob WHERE ob.order_id = o.id)`, true, "已提交订单必须有可追踪事件"},
		{"Outbox SENT 未消费", `SELECT COUNT(*) FROM outbox_events ob
			WHERE ob.status='SENT' AND NOT EXISTS (SELECT 1 FROM inbox_events ib WHERE ib.event_id = ob.event_id)`, false, "重放候选（replay-scope 确定范围）"},
		{"Inbox 孤儿事件", `SELECT COUNT(*) FROM inbox_events ib
			WHERE NOT EXISTS (SELECT 1 FROM outbox_events ob WHERE ob.event_id = ib.event_id)`, true, "Inbox 必须能回溯 Outbox"},
		{"orders 无通知", `SELECT COUNT(*) FROM orders o
			WHERE NOT EXISTS (SELECT 1 FROM order_notifications n WHERE n.order_id = o.id)`, false, "重放完成后应为 0"},
		{"通知无对应订单", `SELECT COUNT(*) FROM order_notifications n
			WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = n.order_id)`, true, "副作用不可超出订单集"},
		{"幂等孤儿（order_id=0）", `SELECT COUNT(*) FROM idempotency WHERE order_id=0`, true, "占位未回填残留"},
		{"Outbox DEAD", `SELECT COUNT(*) FROM outbox_events WHERE status='DEAD'`, false, "显式失败记录，需人工处理"},
	}
	for _, c := range checks {
		var n int64
		if err := db.QueryRow(c.query).Scan(&n); err != nil {
			fatal("%s 查询失败: %v", c.name, err)
		}
		mark := "OK"
		if n > 0 && c.bad {
			mark = "VIOLATION"
			fail++
		}
		fmt.Printf("  [%-9s] %-30s %8d   (%s)\n", mark, c.name, n, c.note)
		if verbose && n > 0 {
			printSample(db, c.query, c.name)
		}
	}

	// 库存守恒差分：Δorders + Δstock == 0 per SKU（「初始库存＝剩余库存＋已提交订单数」的差分形式，
	// 不依赖知道初始库存绝对值——实验期间多次重置过 P1 库存，绝对值不可靠）
	if baselinePath != "" {
		fmt.Printf("== 库存守恒差分（对照基线 %s）==\n", baselinePath)
		raw, err := os.ReadFile(baselinePath)
		if err != nil {
			fatal("读基线失败: %v", err)
		}
		var base snapshot
		if err := json.Unmarshal(raw, &base); err != nil {
			fatal("解析基线失败: %v", err)
		}
		cur, err := currentSKUWater(db)
		if err != nil {
			fatal("SKU 水位失败: %v", err)
		}
		skus := map[string]bool{}
		for k := range base.SKU {
			skus[k] = true
		}
		for k := range cur {
			skus[k] = true
		}
		list := make([]string, 0, len(skus))
		for k := range skus {
			list = append(list, k)
		}
		sort.Strings(list)
		broken := 0
		for _, sku := range list {
			b, c := base.SKU[sku], cur[sku]
			if (c.Orders-b.Orders)+(c.Stock-b.Stock) != 0 {
				fmt.Printf("  守恒破坏 %s：Δorders=%d Δstock=%d\n", sku, c.Orders-b.Orders, c.Stock-b.Stock)
				broken++
				if broken >= 20 {
					fmt.Println("  ...（截断）")
					break
				}
			}
		}
		if broken == 0 {
			fmt.Printf("  OK：全部 %d 个 SKU 差分守恒（Δorders+Δstock=0）\n", len(list))
		} else {
			fail++
		}
	}

	// 账本逐单核验：「客户端已收到成功响应的订单，仍可查询」的判定性证据
	if ledgerPath != "" {
		fmt.Printf("== 账本逐单核验（%s）==\n", ledgerPath)
		entries := loadLedger(ledgerPath)
		if len(entries) == 0 {
			fmt.Println("  账本为空，跳过")
		} else {
			// 批量载入 k6 用户域的订单与幂等行（避免逐单往返；账本可达数万条）
			type orderRow struct {
				user, sku string
				createdAT time.Time
			}
			ordersByID := map[int64]orderRow{}
			rows, err := db.Query(`SELECT id, user_id, sku, created_at FROM orders WHERE user_id LIKE 'fr-u-%'`)
			if err != nil {
				fatal("载入订单失败: %v", err)
			}
			for rows.Next() {
				var id int64
				var r orderRow
				if err := rows.Scan(&id, &r.user, &r.sku, &r.createdAT); err != nil {
					fatal("%v", err)
				}
				ordersByID[id] = r
			}
			rows.Close()
			type idemKey struct{ user, key string }
			idemOrder := map[idemKey]int64{}
			rows, err = db.Query(`SELECT user_id, idem_key, order_id FROM idempotency WHERE user_id LIKE 'fr-u-%'`)
			if err != nil {
				fatal("载入幂等行失败: %v", err)
			}
			for rows.Next() {
				var k idemKey
				var oid int64
				if err := rows.Scan(&k.user, &k.key, &oid); err != nil {
					fatal("%v", err)
				}
				idemOrder[k] = oid
			}
			rows.Close()

			var lost, mismatch, ghost int64
			var lostList []ledgerEntry
			for _, e := range entries {
				r, ok := ordersByID[e.orderID]
				if !ok {
					lost++
					lostList = append(lostList, e)
					continue
				}
				if r.user != e.user || r.sku != e.sku {
					mismatch++
					if verbose {
						fmt.Printf("  mismatch order=%d ledger(%s,%s) db(%s,%s)\n", e.orderID, e.user, e.sku, r.user, r.sku)
					}
					continue
				}
				if oid := idemOrder[idemKey{e.user, e.idem}]; oid != e.orderID {
					mismatch++
					if verbose {
						fmt.Printf("  idem mismatch order=%d key=%s idem.order=%d\n", e.orderID, e.idem, oid)
					}
				}
			}
			// 在窗未确认订单：账本窗口开始后创建、但不在账本中的订单——
			// 「提交成功但响应丢失/超时」的合法状态（客户端应以原幂等键重试收敛），
			// 报告为信息项而非违约；窗口前的历史订单不属于本账本覆盖范围。
			ledgerIDs := map[int64]bool{}
			for _, e := range entries {
				ledgerIDs[e.orderID] = true
			}
			var ghostList []int64
			minTs := time.UnixMilli(entries[0].tsMs).Add(-30 * time.Second)
			for id, r := range ordersByID {
				if r.createdAT.After(minTs) && !ledgerIDs[id] {
					ghost++
					ghostList = append(ghostList, id)
				}
			}
			verified := int64(len(entries)) - lost - mismatch
			fmt.Printf("  账本唯一订单 %d：verified=%d lost=%d mismatch=%d\n", len(entries), verified, lost, mismatch)
			fmt.Printf("  在窗未确认订单（DB 有、账本无，合法的响应未知态）：%d\n", ghost)
			if len(ghostList) > 0 && verbose {
				for i, id := range ghostList {
					if i >= 10 {
						fmt.Println("    ...（截断）")
						break
					}
					fmt.Printf("    unconfirmed order=%d\n", id)
				}
			}
			if mismatch > 0 {
				fail++
			}
			if lost > 0 {
				if strict {
					fail++
				}
				target := os.Stdout
				if lostFile != "" {
					if f, err := os.Create(lostFile); err == nil {
						target = f
						defer f.Close()
					}
				}
				fmt.Fprintln(target, "# 丢失订单清单（账本有成功响应、恢复数据中不存在）")
				for _, e := range lostList {
					fmt.Fprintf(target, "LEDGER|%d|%s|%s|%s|%d\n", e.orderID, e.user, e.sku, e.idem, e.tsMs)
				}
				fmt.Printf("  丢失清单已写出（%d 条）\n", len(lostList))
			}
		}
	}

	fmt.Println()
	if fail > 0 {
		fmt.Printf("RESULT: FAIL（%d 类违约）\n", fail)
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS")
}

func currentSKUWater(db *sql.DB) (map[string]skuWater, error) {
	out := map[string]skuWater{}
	// 单事务（InnoDB 默认 REPEATABLE READ）：orders 分组与 inventory 读取看到同一
	// 一致性快照。负载运行中若分开采样，两查询间隙的提交会引入假差分
	//（Δ = 间隙内订单数），破坏守恒校验的判定力。
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT o.sku, COUNT(*) cnt FROM orders o GROUP BY o.sku`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var sku string
		var cnt int64
		if err := rows.Scan(&sku, &cnt); err != nil {
			return nil, err
		}
		out[sku] = skuWater{Orders: cnt}
	}
	rows.Close()
	rows, err = tx.Query(`SELECT sku, stock FROM inventory`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var sku string
		var stock int64
		if err := rows.Scan(&sku, &stock); err != nil {
			return nil, err
		}
		w := out[sku]
		w.Stock = stock
		out[sku] = w
	}
	rows.Close()
	return out, nil
}

func printSample(db *sql.DB, query, name string) {
	q := strings.Replace(query, "SELECT COUNT(*)", "SELECT *", 1) + " LIMIT 3"
	rows, err := db.Query(q)
	if err != nil {
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	i := 0
	for rows.Next() && i < 3 {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for j := range vals {
			ptrs[j] = &vals[j]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return
		}
		parts := make([]string, len(cols))
		for j, v := range vals {
			if b, ok := v.([]byte); ok {
				parts[j] = string(b)
			} else {
				parts[j] = fmt.Sprintf("%v", v)
			}
		}
		fmt.Printf("    %s 样本: %s\n", name, strings.Join(parts, " | "))
		i++
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "reconcile: "+format+"\n", args...)
	os.Exit(2)
}

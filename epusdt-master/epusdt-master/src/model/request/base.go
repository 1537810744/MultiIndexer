package request

const (
	OrderByFuncDesc = "DESC"
	OrderByFuncAsc  = "OrderByFuncASC"
)

var OrderByFuncList = []string{OrderByFuncDesc, OrderByFuncAsc}

type BaseRequest struct {
	Page       int    `json:"page" example:"1"`              // 页数
	PageSize   int    `json:"page_size" example:"20"`        // 每页条数
	OrderField string `json:"order_field" example:"created_at"` // 排序字段
	OrderFunc  string `json:"order_func" enums:"DESC,ASC" example:"DESC"` // 排序方法
}

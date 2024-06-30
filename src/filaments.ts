import {Knex} from "knex";
import * as Joi from "joi";
import {NumberSchema, StringSchema} from "joi";
import * as _ from 'lodash'


// Todo Array的兼容
export type MakeType<S> = {
    [K in keyof S]?:
    S[K] extends StringSchema ? String :
        S[K] extends NumberSchema ? number :
            S[K] extends Object ? MakeType<S[K]>:
                never
    ;
};

type Ids = number | number[] | string | string[]
type Sub = Knex.QueryBuilder | null

// Todo 能否兼容外部框架调用
type Query = {
    pc?: number;
    p?: number;
    od?:  string | string[];
    rt?:  string | string[];
    or?: string | string[][];
    pg?: number;
    lg?: string;
    // Todo 其他的保留字符串
    [key: string]: number| number[] | undefined | string | string[] | string[][]
}



type AggregationTarget = {
    sum?: string | string[],
    count?: string | string[],
}

export class Filaments<T> {
    static DEFAULT_QUERY_PC: number = 20
    static DEFAULT_QUERY_P: number = 1
    static NAME_SPLITTER: string = '|'
    static VALUE_SPLITTER: string = '|'

    public json_fields: string[] = []
    public maps: {} = {}
    public before: Function | null = null
    public after: Function | null = null
    public schema: object = {}
    public table: string = ''
    public pk: string = 'id'

    constructor(table: string, schema: object, maps: object) {
        this.table = table
        this.schema = schema
        this.maps = maps

        _.forEach(schema, (value, key) => {
            if (_.isObject(value)) {
                this.json_fields.push(key)
            }
        })
    }



    // region 基础
    /**
     * 默认的校验处理器
     */
    protected default_schema_handler = (S: object, data: T) => {
        return _.pick(S, _.keys(data))
    }

    /**
     * JSON字段处理
     */
    protected json_handler = (data: any, func: Function): any => {
        if (this.json_fields.length > 0) {
            if (_.isArray(data)) {
                return _.map(data, (obj)=>{
                    return this.json_handler(obj, func)
                })
            } else {
                return _.mapValues(data, (val: any, key: String) => {
                    if (_.includes(this.json_fields, key)) val = func(val)
                    return val
                });
            }
        } else {
            return data;
        }
    }

    /**
     * 处理Joi校验格式
     */
    public normalize_schema = (schema: object): Joi.Schema => {
        const obj = _.mapValues(schema, (val) => {

            if (_.isObject(val) && !Joi.isSchema(val)) {
                return this.normalize_schema(val)
            } else {
                return val
            }
        })

        return Joi.object(obj)
    }

    // Todo
    private field_name_safe(field_name: string): string {
        return field_name
    }
    private func_name_save(func_name: string): string {
        return func_name
    }
    // endregion

    // region 新增
    public async create(db: Knex, data: T[] | T, schema_handle: Function | null = null) {
        let copy: any = _.cloneDeep(data)

        const single = !_.isArray(copy)
        if (single) {
            copy = [copy]
        }

        // region 处理验证器
        let schema = _.cloneDeep(this.schema)
        schema = schema_handle ? schema_handle(schema) : this.default_schema_handler(schema, copy);
        schema = this.normalize_schema(schema)
        // endregion

        // region 处理数据
        const final = _.map(copy, (val, key) => {
            val = this.before ? this.before(val, key) : val

            const res = Joi.object(schema).validate(val, {
                presence: 'required',
            })
            if (res.error) {
                throw res.error.message + (single ? '' : `(${key})`)
            }

            val = this.json_handler(val, JSON.stringify)

            return val
        })
        // endregion

        return db.insert(final).into(this.table)
    }
    // endregion

    // region 删除

    public delete_by_ids(db: Knex, ids: Ids) {
        // Todo 好像IDs没必要clone
        let copy: any = _.clone(ids)

        if (!_.isArray(copy)) {
            copy = [copy]
        }

        return db.table(this.table).whereIn(this.pk, copy).del()
    }
    // endregion
    // region 修改
    public update_by_ids(db: Knex, ids: Ids, data: T, schema_handler: Function | null = null) {
        let copy = _.cloneDeep(data)
        let copy_ids: any = _.clone(ids)

        if (!_.isArray(copy_ids)) {
            copy_ids = [copy_ids]
        }

        let schema = _.cloneDeep(this.schema)
        schema = schema_handler ? schema_handler(schema) : this.default_schema_handler(schema, copy);

        const res = Joi.object(schema).validate(copy, {
            presence: 'required',
        })
        if (res.error) {
            throw res.error.message
        }


        const final =  this.json_handler(copy, JSON.stringify)

        return db.table(this.table).whereIn(this.pk, copy_ids).update(final)
    }
    // endregion

    // region 查询

    // region 共用

    public build_return(db: Knex.QueryBuilder, query: Query): Knex.QueryBuilder {
        let fields: any = '*'
        if (query.rt) {
            fields = _.isArray(query.rt) ? query.rt : query.rt.split(',');
        }

        return db.select(fields)
    }

    public build_sub(db: Knex, sub: Sub) : Knex.QueryBuilder{
        if (sub) {
            return db.table(db.raw(sub.table(this.table)).wrap('(', ') as sub'))
        } else {
            return db.table(this.table)
        }
    }

    public build_order(db: Knex.QueryBuilder, query: Query) {
        if (query.od) {
            const sorts: string[] = []
            const origin: string[] = _.isArray(query.od) ? query.od : query.od.split(',')
            _.forEach(origin, (val) => {
                let order, field
                if (_.startsWith(val, '-')) {
                    order = 'DESC'
                    field = val.substr(1)
                } else {
                    order = 'ASC'
                    field = val
                }

                sorts.push(`\`${field}\` ${order}`)
            })

            return db.orderByRaw(sorts.join(','))
        } else {
            return db
        }
    }

    /**
     * 构建查询条件
     */
    public build_condition(db: Knex.QueryBuilder, query: Query) {
        // Todo jhas  jhm jnin jbet

        class LogicTreeNode {
            public type: "or" | "and" = "and";
            public value: string[] | LogicTreeNode[] = []
            public str: string = ""
            public all_child: string[] = []
            public children: LogicTreeNode[] = []
        }
        const parseLogic = (input: string): LogicTreeNode => {
            // Todo 支持""包裹字符串
            const root = new LogicTreeNode()
            let current = root
            const stack = [root]
            let str_wrap = ''
            for (let i = 0 ; i < input.length ; i++) {
                const char = input[i]
                if (char == '(') {
                    const prev = input[i-1]
                    const type = prev == '!' ? 'or' : 'and'
                    const node = new LogicTreeNode()
                    node.type = type

                    current.children?.push(node)
                    current.str += `?${current.children.length-1}`

                    stack.push(node)
                    current = node
                } else if (char == ')') {
                    // region 处理最终数据结构
                    const all_child = new Set()
                    for (const key of current.str?.split(',')) {
                        if (key[0] == '?') {
                            const index = Number.parseInt(key.substring(1))
                            current.value.push(current.children[index] as any)

                            for (const child of current.children) {
                                for (const key of child.all_child) {
                                    all_child.add(key)
                                }
                            }
                        } else {
                            all_child.add(key)
                            current.value.push(key as any)
                        }
                    }
                    current.all_child = Array.from(all_child) as string[]

                    // endregion

                    stack.pop()
                    current = stack[stack.length - 1]
                } else {
                    if (char != '!') {
                        current.str += char
                    }
                }
            }

            return root.children[0]
        }


        let base = db

        const filtered_query = _.omit(query, ['p', 'pc', 'od', 'rt', 'gp', 'pg', 'lg'])

        // Todo 这里的类型要限制的严格一些
        const array_val = (val: any) => (!_.isArray(val) ? val.split(',') : val)
        const make_holder = (val: any)=> _.join(_.map(val, () => '?'))


        let logic_tree = new LogicTreeNode()
        if (query.lg) {
            logic_tree = parseLogic(query.lg)
        }
        // 未明确声明的field设为默认值
        logic_tree.value.push(..._.keys(_.omit(filtered_query, logic_tree.all_child)) as any)

        /**
         * 字段和条件构建
         */
        const field_build = (ctx: Knex.QueryBuilder, picked_query: Query,  where_type: "orWhereRaw" | "andWhereRaw"): Knex.QueryBuilder => {
            _.forEach(picked_query, (val, key: string) => {
                const left_parts = key.split(Filaments.NAME_SPLITTER)
                let field = left_parts[0]
                let op = ''
                let func_list: String[] = []

                // Todo 更新完整的操作符名单
                let op_str = ['le', 'lt', 'in']
                const last_part = _.snakeCase(_.last(left_parts))
                if (op_str.indexOf(last_part) > -1) {
                    op = last_part
                    func_list = _.slice(left_parts, 1, left_parts.length - 1)
                } else {
                    func_list = _.slice(left_parts, 1)
                }

                // 数值
                // 函数参数
                // 统一转化为数组
                let value = array_val(val)

                // json和普通字段统一处理, // TOdo 去除名字内部的反引号
                let sql_field = `\`${field}\``
                if (field.indexOf('.') > -1 || field.indexOf('[') > -1) {
                    const segments = field.split('.')
                    sql_field = `\`${segments[0]}\`->'$.${segments.slice(1).join('.')}'`
                }

                // region 处理函数调用
                for (const func of func_list) {
                    let param_list: string[] = []
                    if (func.indexOf('(') > -1) {
                        param_list = func.substring(func.indexOf('(') + 1, func.indexOf(')')).split(',')
                        // Todo 数据类型问题
                        // Todo 参数安全问题
                    }

                    const param_str = _.isEmpty(param_list) ? '' : param_list.join(',')
                    sql_field = `${func}(${sql_field}${param_str})`
                }
                // endregion

                // region 处理比较操作符
                const where_func: Knex.RawQueryBuilder = _.bind(ctx[where_type], ctx)
                // 根据不同的函数和操作符拼接
                //TODO 同名参数如何处理
                // Todo URIEncode问题
                switch (op) {
                    case '':
                        ctx = where_func(`${sql_field} = ?`, _.take(value))
                        break;
                    case 'ge':
                        ctx = where_func(`${sql_field} >= ?`, _.take(value))
                        break
                    case 'gt':
                        ctx = where_func(`${sql_field} > ?`, _.take(value))
                        break
                    case 'le':
                        ctx = where_func(`${sql_field} <= ?`, _.take(value))
                        break
                    case 'lt':
                        ctx = where_func(`${sql_field} < ?`, _.take(value))
                        break
                    case 'in':
                        ctx = where_func(`${sql_field} in (${make_holder(value)})`, value)
                        break
                    case 'not_in':
                        ctx = where_func(`${sql_field} not in (${make_holder(value)})`, value)
                        break
                    // Todo 多重区间？
                    case 'between':
                        ctx = where_func(`${sql_field} between (${make_holder(value)})`, value)
                        break
                    case 'not_between':
                        ctx = where_func(`${sql_field} not between (${make_holder(value)})`, value)
                        break
                    case 'like':
                        ctx = where_func(`${sql_field} like ?`, _.take(value))
                        break
                    case 'not_like':
                        ctx = where_func(`${sql_field} not like ?`, _.take(value))
                        break
                    case 'is_null':
                        ctx = where_func(`${sql_field} is null`)
                        break
                    case 'is_not_null':
                        ctx = where_func(`${sql_field} is not null`)
                        break

                }

            })
            return ctx
        }

        /**
         * 逻辑树构建
         */
        const tree_build = (ctx: Knex.QueryBuilder, node: LogicTreeNode): Knex.QueryBuilder => {
            const tree_func_type: "orWhere" | "andWhere" = node.type == "and" ? "andWhere" : "orWhere"
            const field_func_type: any = tree_func_type + "Raw"

            return ctx[tree_func_type]((sub_ctx)=> {
                for (const child of node.value) {
                    if (child instanceof LogicTreeNode) {
                        sub_ctx = tree_build(sub_ctx, child)
                    } else {
                        // 常规构建
                        sub_ctx = field_build(sub_ctx, _.pick(query, child), field_func_type)
                    }
                }
            })
        }

        base = tree_build(base, logic_tree)


        return base
    }

    protected build_select(db: Knex, query: Query, sub: Sub) {
        let ctx = this.build_sub(db, sub)
        ctx = this.build_return(ctx, query)

        ctx = this.build_condition(ctx, query)
        ctx = this.build_order(ctx, query)

        return ctx
    }

    protected async do_query (query: Knex.QueryBuilder, lock: boolean = false): Promise<T[]>{
        let data: any = await (lock ? query.forUpdate() : query)

        data = this.json_handler(data, JSON.parse)
        if (this.after) {
            data = _.map(data, this.after)
        }
        return data
    }

    // endregion

    // region 直接查询

    /**
     * 条件查询
     */
    public async get(db: Knex, query: Query, sub: Sub = null) {
        let base = this.build_select(db, query, sub)

        if (query.pc) {
            base = base.limit(query.pc)
        }

        return await this.do_query(base)
    }

    /**
     * id查询
     */
    public async get_by_ids(db: Knex, ids: Ids, lock: boolean = false) {
        const arr = _.isArray(ids) ? ids : [ids]
        const query = db.table(this.table).whereIn(this.pk, arr)
        return await this.do_query(query, lock)
    }


    /**
     * 分页查询
     */
    public async pages(db: Knex, query: Query, sub: Sub = null) {
        const copy = _.cloneDeep(query)
        if (!copy.p) {
            copy.p = Filaments.DEFAULT_QUERY_P
        }
        if (!copy.pc) {
            copy.pc = Filaments.DEFAULT_QUERY_PC
        }

        if (copy.p <= 0) throw '页码请从第一页开始'

        let base = this.build_select(db, copy, sub)
            .limit(copy.pc)
            .offset((copy.p - 1) * copy.pc)

        const rows = await this.do_query(base)

        let count = 0
        if (Number(copy.pg) > 0) {
            const res = await this.aggregation(db, {count: ['id']}, copy, [], sub)
            count = res[0]['count_id']
        }


        return {
            data: rows,
            count,
            pages: {
                total: Math.ceil(count / copy.pc),
                now: Number(copy.p),
            },
        }
    }

    // endregion



    // region 聚合
    public aggregation(db: Knex, target: AggregationTarget, query: Query = {}, group: string | string[] = [], sub: Sub = null ): Knex.QueryBuilder{
        let base = this.build_select(db, query, sub).clearSelect()

        if (!_.isEmpty(group)) {
            base = base.groupBy(_.isArray(group) ? group : [group])
        }

        _.forEach(target, (val: any, func) => {
            if (!_.isArray(val)) {val = [val]}
            for (const field of val) {
                const func_name = this.func_name_save(func)
                const field_name = this.field_name_safe(field)

                base = base.select(db.raw(`${func_name}(\`${field_name}\`) as ${func_name}_${field_name}`))
            }
        })

        return base.select(group);
    }
    // endregion

    // endregion


    /**
     * 直接返回knex.QueryBuilder,可以根据需要追加参数
     * 1.可以通过Mysql2驱动的 .options({rowsAsArray: true}) 返回数组
     * 2.可以通过.stream返回流
     */
    public get_raw(db: Knex, query: Query, sub: Sub = null): Knex.QueryBuilder {
        let base = this.build_select(db, query, sub)
        if (query.pc) {
            base = base.limit(query.pc)
        }

        return base
    }
}
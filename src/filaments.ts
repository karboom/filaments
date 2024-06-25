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
    or?: string | string[][]
    // Todo 其他的保留字符串
    [key: string]: number| number[] | undefined | string | string[] | string[][]
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
     * 处理Joi校验格式, Todo 测试
     */
    protected normalize_schema = (schema: object): object => {
        const obj = _.mapValues(schema, (val) => {
            if (_.isObject(val)) {
                return this.normalize_schema(val)
            } else {
                return val
            }
        })

        return Joi.object(obj)
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

        return db.table(this.table).whereIn('id', copy).del()
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

        return db.table(this.table).whereIn('id', copy_ids).update(final)
    }
    // endregion

    // region 查询

    // region 共用


    protected build_return(db: Knex.QueryBuilder, query: Query): Knex.QueryBuilder {
        let fields: any = '*'
        if (query.rt) {
            fields = _.isArray(query.rt) ? query.rt : query.rt.split(',');
        }

        return db.select(fields)
    }

    protected build_sub(db: Knex, sub: Sub) : Knex.QueryBuilder{
        if (sub) {
            return db.table(db.raw(sub.table(this.table)).wrap('(', ') as sub'))
        } else {
            return db.table(this.table)
        }
    }

    protected build_order(db: Knex.QueryBuilder, query: Query) {
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
     * @param db
     * @param query
     * @protected
     */
    protected build_condition(db: Knex.QueryBuilder, query: Query) {
        // Todo jhas  jhm jnin jbet
        const parseStringToNDArray = (input: string): any[] => {
            // 去除字符串两端的空格
            input = input.trim();

            // 如果字符串为空，返回空数组
            if (input === '') return [];

            // 如果字符串不以'('开头或不以')'结尾，抛出错误
            if (!input.startsWith('(') || !input.endsWith(')')) {
                throw new Error("Invalid input format.");
            }

            // 递归解析函数
            const parse = (str: string): any[] => {
                // 去除两端的括号
                str = str.slice(1, -1).trim();

                // 如果字符串为空，返回空数组
                if (str === '') return [];

                const result = [];
                let currentElement = '';
                let nestedLevel = 0;

                for (let i = 0; i < str.length; i++) {
                    const char = str[i];

                    if (char === '(') {
                        if (nestedLevel === 0 && currentElement.trim() !== '') {
                            throw new Error("Invalid input format.");
                        }
                        nestedLevel++;
                        currentElement += char;
                    } else if (char === ')') {
                        nestedLevel--;
                        currentElement += char;
                        if (nestedLevel === 0) {
                            result.push(parse(currentElement));
                            currentElement = '';
                        }
                    } else if (char === ',' && nestedLevel === 0) {
                        if (currentElement.trim() !== '') {
                            result.push(currentElement.trim());
                        }
                        currentElement = '';
                    } else {
                        currentElement += char;
                    }
                }

                if (nestedLevel !== 0) {
                    throw new Error("Invalid input format.");
                }

                if (currentElement.trim() !== '') {
                    result.push(currentElement.trim());
                }

                return result;
            }

            return parse(input);
        }

        let base = db

        const filtered_query = _.omit(query, ['p', 'pc', 'od', 'rt', 'sub', 'gp', 'pg', 'or'])

        // Todo 这里的类型要限制的严格一些
        const array_val = (val: any) => (!_.isArray(val) ? val.split(',') : val)
        const make_holder = (val: any)=> _.join(_.map(val, () => '?'))


        let or_fields = []
        if (query.or) {
            if (_.isString(query.or)) {
                or_fields = parseStringToNDArray(query.or)
            }
            if (_.isArray(query.or)) {
                or_fields = query.or
            }
        }
        const key_group = _.groupBy(_.keys(filtered_query), (val)=> {
            const index = _.findIndex(or_fields, (v)=> v.indexOf(val) > -1)

            if (index >= 0) {
                return `negative_${index+1}`
            } else {
                // 默认情况下，&为正向
                return `positive`
            }
        })

        // 根据不同的逻辑类型构建
        // TODO 是否支持 更复杂的 运算逻辑，比如说反过来 默认用 | 指定的分组采用 &
        _.forEach(key_group, (keys, type)=> {
            const where_type = type.indexOf('negative') > -1 ? 'orWhere' : 'andWhere';

            base = base.andWhere((ctx)=> {
                _.forEach(_.pick(filtered_query, keys), (val, key) => {
                    const left_parts = key.split(Filaments.NAME_SPLITTER)
                    let field = left_parts[0]
                    let op = ''
                    let func_list: String[] = []

                    // Todo 更新完整的操作符名单
                    let op_str = ['le', 'lt']
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


                    // json和普通字段统一处理
                    let sql_field = `\`${field}\``
                    if (field.indexOf('.')>-1 || field.indexOf('[')>-1) {
                        const segments = field.split('.')
                        sql_field = `\`${segments[0]}\`->'$.${segments.slice(1).join('.')}'`
                    }

                    // region 处理函数调用
                    for (const func of func_list) {
                        let param_list: string[] = []
                        if (func.indexOf('(') > -1) {
                            param_list = func.substring(func.indexOf('(')+1, func.indexOf(')')).split(',')
                            // Todo 数据类型问题
                            // Todo 参数安全问题
                        }

                        const param_str = _.isEmpty(param_list) ? '' : param_list.join(',')
                        sql_field = `${func}(${sql_field}${param_str})`
                    }
                    // endregion

                    // region 处理比较操作符
                    const where_func: Knex.RawQueryBuilder = ctx[where_type]
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
                        case 'between':
                            ctx = where_func(`${sql_field} between (${make_holder(value)})`, value)
                            break
                        case 'not_between':
                            ctx = where_func(`${sql_field} not between (${make_holder(value)})`, value)
                            break
                        case 'like':
                            ctx = where_func(`${sql_field} like ?`, _.take(value))
                            break
                        case 'not like':
                            ctx = where_func(`${sql_field} not like ?`, _.take(value))
                            break
                        case 'is_null':
                            ctx = where_func(`${sql_field} is null`)
                            break
                        case 'is_not_null':
                            ctx = where_func(`${sql_field} is not null`)
                            break

                    }
                    // endregion
                })

            })
        })


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
        const query = db.table(this.table).whereIn('id', arr)
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
            const res = await this.aggregation(db, {id: 'count'}, [], copy, sub)
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
    // Todo 反转target，确保更多可能性
    public async aggregation(db: Knex, target: {[key: string]: 'count' | 'sum'}, group: string[], query: Query, sub: Sub = null ) : Promise<any[]>{
        let base = this.build_select(db, query, sub).clearSelect()

        if (!_.isEmpty(group)) base = base.groupBy(group)

        for (const func of _.keys(target)) {
            // Todo 数组
            const fields = target[func]
            for (const field of fields) {
                let params = `\`${field}\``
                // // TOdo isnumber的稳定性
                // if (_.isnumber(field)) {
                //    params = `${field}`
                // }

                // Todo 转译函数名或者增加反引号
                base = base.select(db.raw(`${func}(\`${field}\`) as ${func}_${field}`))
            }

        }

        const res = await base.select(group)

        return res
    }
    // endregion

    // endregion


    /**
     * 直接返回knex.QueryBuilder,可以根据需要追加参数
     * 1.可以通过Mysql2驱动的 .options({rowsAsArray: true}) 返回数组
     * 2.可以通过.stream返回流
     */
    public get_raw(db: Knex, query: Query, sub: Sub = null) {
        let base = this.build_select(db, query, sub)
        if (query.pc) {
            base = base.limit(query.pc)
        }

        return base
    }
}
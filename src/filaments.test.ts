import {Filaments} from "./filaments";
import * as _ from "lodash";
import * as Joi from "joi";
import {expect} from "chai"
import knex, {Knex} from "knex";
// should()
describe("filaments", () => {
    const db = knex({client: 'mysql'})
    const model = new Filaments("T", {}, {})
    const table = db.table("T").select()

    describe(".normalize_schema", () => {
        it("ok", () => {
            const data1= {
                A: {
                    B: {
                        C: {},
                        X: Joi.number()
                    }
                },

            }

            const res1: any = model.normalize_schema(data1)

            const x = res1.extract('A.B.C').describe().type
                expect(x).eq('object')
            const res1_desc = res1.describe()
            // res1_desc.A.should.be.a("Joi")
        })
    })

    describe(".build_condition", () => {
        it("ok", () => {
            const res = model.build_condition(db.table("23").select(), {p: 1, asd: "2"})
            const x = res.toSQL()
            expect(res.toSQL()).eq(1)
        })
    })

    describe(".aggregation", () => {
        it("no target", () => {
            const res = model.aggregation(db, {}).toSQL()
            expect(res.sql).eq("select * from `T`")
        })
        it("only target", () => {
            const res = model.aggregation(db, {sum: ['amount', 'age'], count: 'id'}).toSQL()
            expect(res.sql).eq("select sum(`amount`) as sum_amount, sum(`age`) as sum_age, count(`id`) as count_id from `T`")
        })
        it("target & query", () => {
            const res = model.aggregation(db, {count: 'id'}, {type: 'A'}).toSQL()
            expect(res.sql).eq("select count(`id`) as count_id from `T` where (`type` = ?)")
        })
        it("target & query & group", () => {
            const res = model.aggregation(db, {count: 'id'}, {type: 'A'}, 'category').toSQL()
            expect(res.sql).eq("select count(`id`) as count_id, `category` from `T` where (`type` = ?) group by `category`")
        })

    })
})
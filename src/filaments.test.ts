import {Filaments} from "./filaments";
import * as Joi from "joi";
import {expect} from "chai"
import knex from "knex";


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
        it('lg', () => {
            const res1 = model.build_condition(table.clone(), {name: "A", 'id|in': '1', lg: `(!(id|in),id|in)`}).toSQL()
            expect(res1.sql).eq('select * from `T` where (`name` = ? or `id` = ?)')
        })

        // it("lb", () => {
        //     const res = model.build_condition(table.clone(), {name: "A", id: '1', lb: 'or'}).toSQL()
        //     expect(res.sql).eq('select * from `T` where (`name` = ? or `id` = ?)')
        //
        //     const res2 = model.build_condition(table.clone(), {name: "A", id: '1', lb: 'and'}).toSQL()
        //     expect(res2.sql).eq('select * from `T` where (`name` = ? and `id` = ?)')
        // })
        // it("lr", () => {
        //     const res = model.build_condition(table.clone(), {name: "A", id: '1', weight: '20', lb: 'or', lr: [['name', 'weight']]}).toSQL()
        //     expect(res.sql).eq('select * from `T` where (`name` = ? and `weight` = ?) or (`id` = ?)')
        //
        //     const res2 = model.build_condition(table.clone(), {name: "A", id: '1', weight: '20', lb: 'and', lr: [['name', 'weight']]}).toSQL()
        //     expect(res2.sql).eq('select * from `T` where (`name` = ? or `weight` = ?) and (`id` = ?)')
        //
        //     const res3 = model.build_condition(table.clone(), {name: "A", id: '1', weight: '20', lb: 'and', lr: '((name,weight))'}).toSQL()
        //     expect(res3.sql).eq('select * from `T` where (`name` = ? or `weight` = ?) and (`id` = ?)')
        // })


        it("func call", () => {
            const res1 = model.build_condition(table.clone(), {id: "1"}).toSQL()
            expect(res1.sql).eq('select * from `T` where (`id` = ?)')

            const res2 = model.build_condition(table.clone(), {"id|date": "1"}).toSQL()
            expect(res2.sql).eq('select * from `T` where (date(`id`) = ?)')

            const res3 = model.build_condition(table.clone(), {"id|date(2)": "1"}).toSQL()
            expect(res3.sql).eq('select * from `T` where (date(`id`, 2) = ?)')

            // Todo 安全性测试
        })

        it("op handle", () => {
            const res1 = model.build_condition(table.clone(), {"data.id": "1"}).toSQL()
            expect(res1.sql).eq('select * from `T` where (`data`->\'$.id\' = ?)')

            const res2 = model.build_condition(table.clone(), {"data.id|ge": "1"}).toSQL()
            expect(res2.sql).eq('select * from `T` where (`data`->\'$.id\' >= ?)')


            const res3 = model.build_condition(table.clone(), {"id|in": "1,2"}).toSQL()
            expect(res3.sql).eq('select * from `T` where (`id` in (? , ?))')

            const res4 = model.build_condition(table.clone(), {"id|between": "1,2"}).toSQL()
            expect(res4.sql).eq('select * from `T` where (`id` between ? and ?)')

            const res = model.build_condition(table.clone(), {"data.id|date": "1"}).toSQL()
            expect(res.sql).eq('select * from `T` where (date(`data`->\'$.id\') = ?)')

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
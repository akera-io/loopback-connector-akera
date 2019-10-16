import { DataSource, PersistedModel } from "loopback-datasource-juggler";
import { InitTests } from "../init";
import { Count } from '@loopback/repository';
import * as should from "should";

let ds: DataSource;
let addedWHouses: number[] = [];
let initCount: number;
let Warehouse: typeof PersistedModel;

function createWhouse(index: number): any {
    addedWHouses.push(index + 1);
    return {
        address: `Create address ${index + 1}`,
        address2: `Create address2 ${index + 1}`,
        city: `Create city ${index + 1}`,
        country: `Create country ${index + 1}`,
        phone: `Create phone ${index + 1}`,
        postalcode: `Create postalcode ${index + 1}`,
        state: `Create state ${index + 1}`,
        warehousename: `Create warehousename ${index + 1}`,
        warehousenum: index + 1
    };
}

function updateWhouse(index: number, updateMsg: string): any {
    return {
        address: `${updateMsg} address ${index + 1}`,
        address2: `${updateMsg} address2 ${index + 1}`,
        city: `${updateMsg} city ${index + 1}`,
        country: `${updateMsg} country ${index + 1}`,
        phone: `${updateMsg} phone ${index + 1}`,
        postalcode: `${updateMsg} postalcode ${index + 1}`,
        state: `${updateMsg} state ${index + 1}`,
        warehousename: `${updateMsg} warehousename ${index + 1}`,
        warehousenum: index + 1
    };
}

describe('Test Create, Read, Update, Delete methods', () => {
    before('before tests actions', async () => {
        ds = InitTests.getDataSource();
        Warehouse = ds.getModel('Warehouse') as typeof PersistedModel;
        initCount = await (Warehouse.count() as Promise<number>);

    });

    after('after test actions', async () => {
        if (ds && ds.connected)
            ds.disconnect();
    })

    it('create record using POST method', async () => {

        let wHouse = createWhouse(initCount);

        let cRsp = await (Warehouse.create(wHouse) as Promise<PersistedModel>);

        should(cRsp).be.instanceOf(Warehouse, 'When create records server should send back an object identic with one created');
        should(cRsp).have.properties('address', 'address2', 'city', 'country', 'phone', 'postalcode', 'state', 'warehousename', 'warehousenum');
        should(cRsp['warehousenum'] == initCount + 1).be.equal(true, 'index of record send by server when create record should be equal with one created');

        let auxCount = await (Warehouse.count() as Promise<number>);
        should(auxCount == initCount + 1).be.equal(true, 'after create number of records should be grater by one');

    })

    it('update record using PUT method with all object properties', async () => {
        let updateMsg = 'UpdateAll';
        let wHouse = updateWhouse(initCount, updateMsg);

        let putRsp = await (Warehouse.replaceById(wHouse['warehousenum'], wHouse) as Promise<PersistedModel>);

        should(putRsp).be.instanceOf(Warehouse, 'When update records server should send back an object identic with one updated');
        should(putRsp).have.properties('address', 'address2', 'city', 'country', 'phone', 'postalcode', 'state', 'warehousename', 'warehousenum');
        should(putRsp['address'] == `${updateMsg} address ${initCount + 1}`).be.equal(true, 'property address should be updated');
        should(putRsp['address2'] == `${updateMsg} address2 ${initCount + 1}`).be.equal(true, 'property address2 should be updated');
        should(putRsp['city'] == `${updateMsg} city ${initCount + 1}`).be.equal(true, 'property city should be updated');
        should(putRsp['country'] == `${updateMsg} country ${initCount + 1}`).be.equal(true, 'property country should be updated');
        should(putRsp['phone'] == `${updateMsg} phone ${initCount + 1}`).be.equal(true, 'property phone should be updated');
        should(putRsp['postalcode'] == `${updateMsg} postalcode ${initCount + 1}`).be.equal(true, 'property postalcode should be updated');
        should(putRsp['state'] == `${updateMsg} state ${initCount + 1}`).be.equal(true, 'property state should be updated');
        should(putRsp['warehousename'] == `${updateMsg} warehousename ${initCount + 1}`).be.equal(true, 'property warehousename should be updated');

        let auxCount = await (Warehouse.count() as Promise<number>);
        should(auxCount == initCount + 1).be.equal(true, 'after update number of records should be unmodified');
    })

    it('update record using PUT method with partial object properties', async () => {

        let wHouse: any = {
            address: `Update partial after UpdateAll and Create address ${initCount + 1}`,
            warehousenum: initCount + 1
        }

        let putRsp = await (Warehouse.replaceById(wHouse['warehousenum'], wHouse) as Promise<PersistedModel>);

        should(putRsp).be.instanceOf(Warehouse, 'When update records server should send back an object identic with one updated');
        should(putRsp).have.properties('address', 'address2', 'city', 'country', 'phone', 'postalcode', 'state', 'warehousename', 'warehousenum');
        should(putRsp['address'] == `Update partial after UpdateAll and Create address ${initCount + 1}`).be.equal(true, 'property address should be updated');
        should(putRsp['address2'] == null).be.equal(true, 'property address2 should be null');
        should(putRsp['city'] == null).be.equal(true, 'property city should be null');
        should(putRsp['country'] == null).be.equal(true, 'property country should be null');
        should(putRsp['phone'] == null).be.equal(true, 'property phone should be null');
        should(putRsp['postalcode'] == null).be.equal(true, 'property postalcode should be null');
        should(putRsp['state'] == null).be.equal(true, 'property state should be null');
        should(putRsp['warehousename'] == null).be.equal(true, 'property warehousename should be null');

        let auxCount = await (Warehouse.count() as Promise<number>);
        should(auxCount == initCount + 1).be.equal(true, 'after update number of records should be unmodified');

    })

    it('update record using PATCH method', async () => {
        let wHouse: any = {
            city: `Patch city ${initCount + 1}`,
            phone: `Patch phone ${initCount + 1}`
        }

        let pathcRsp = await (Warehouse.update({ warehousenum: initCount + 1 }, wHouse) as Promise<Count>);

        should(pathcRsp['count'] == 1).be.equal(true, 'after succesful patch update server should return number of records updated');

        let getByIdRsp = await (Warehouse.find({ where: { warehousenum: initCount + 1 } }) as Promise<PersistedModel[]>);

        should(getByIdRsp.length).be.equal(1, 'in case of get by id number of records should be one');
        should(getByIdRsp[0]['address'] == `Update partial after UpdateAll and Create address ${initCount + 1}`).be.equal(true, 'property address should be not modified');
        should(getByIdRsp[0]['address2'] == null).be.equal(true, 'property address2 should be not modified');
        should(getByIdRsp[0]['city'] == `Patch city ${initCount + 1}`).be.equal(true, 'property city should be updated');
        should(getByIdRsp[0]['country'] == null).be.equal(true, 'property country should be not modified');
        should(getByIdRsp[0]['phone'] == `Patch phone ${initCount + 1}`).be.equal(true, 'property phone should be updated');
        should(getByIdRsp[0]['postalcode'] == null).be.equal(true, 'property postalcode should be not modified');
        should(getByIdRsp[0]['state'] == null).be.equal(true, 'property state should be not modified');
        should(getByIdRsp[0]['warehousename'] == null).be.equal(true, 'property warehousename should be not modified');
    })

    it('delete records added in crud test ', async () => {

        addedWHouses.forEach((val) => {
            Warehouse.destroyAll({ warehousenum: val });
        })

        let finalCount = await (Warehouse.count() as Promise<number>);
        should(initCount == finalCount).be.equal(true, 'after tests nuber of record in table should be unmodified');
    })

});
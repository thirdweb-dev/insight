// Code generated by mockery v2.50.4. DO NOT EDIT.

//go:build !production

package mocks

import (
	big "math/big"

	mock "github.com/stretchr/testify/mock"
	common "github.com/thirdweb-dev/indexer/internal/common"

	storage "github.com/thirdweb-dev/indexer/internal/storage"
)

// MockIMainStorage is an autogenerated mock type for the IMainStorage type
type MockIMainStorage struct {
	mock.Mock
}

type MockIMainStorage_Expecter struct {
	mock *mock.Mock
}

func (_m *MockIMainStorage) EXPECT() *MockIMainStorage_Expecter {
	return &MockIMainStorage_Expecter{mock: &_m.Mock}
}

// GetAggregations provides a mock function with given fields: table, qf
func (_m *MockIMainStorage) GetAggregations(table string, qf storage.QueryFilter) (storage.QueryResult[interface{}], error) {
	ret := _m.Called(table, qf)

	if len(ret) == 0 {
		panic("no return value specified for GetAggregations")
	}

	var r0 storage.QueryResult[interface{}]
	var r1 error
	if rf, ok := ret.Get(0).(func(string, storage.QueryFilter) (storage.QueryResult[interface{}], error)); ok {
		return rf(table, qf)
	}
	if rf, ok := ret.Get(0).(func(string, storage.QueryFilter) storage.QueryResult[interface{}]); ok {
		r0 = rf(table, qf)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[interface{}])
	}

	if rf, ok := ret.Get(1).(func(string, storage.QueryFilter) error); ok {
		r1 = rf(table, qf)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetAggregations_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetAggregations'
type MockIMainStorage_GetAggregations_Call struct {
	*mock.Call
}

// GetAggregations is a helper method to define mock.On call
//   - table string
//   - qf storage.QueryFilter
func (_e *MockIMainStorage_Expecter) GetAggregations(table interface{}, qf interface{}) *MockIMainStorage_GetAggregations_Call {
	return &MockIMainStorage_GetAggregations_Call{Call: _e.mock.On("GetAggregations", table, qf)}
}

func (_c *MockIMainStorage_GetAggregations_Call) Run(run func(table string, qf storage.QueryFilter)) *MockIMainStorage_GetAggregations_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(storage.QueryFilter))
	})
	return _c
}

func (_c *MockIMainStorage_GetAggregations_Call) Return(_a0 storage.QueryResult[interface{}], _a1 error) *MockIMainStorage_GetAggregations_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockIMainStorage_GetAggregations_Call) RunAndReturn(run func(string, storage.QueryFilter) (storage.QueryResult[interface{}], error)) *MockIMainStorage_GetAggregations_Call {
	_c.Call.Return(run)
	return _c
}

// GetBlockHeadersDescending provides a mock function with given fields: chainId, from, to
func (_m *MockIMainStorage) GetBlockHeadersDescending(chainId *big.Int, from *big.Int, to *big.Int) ([]common.BlockHeader, error) {
	ret := _m.Called(chainId, from, to)

	if len(ret) == 0 {
		panic("no return value specified for GetBlockHeadersDescending")
	}

	var r0 []common.BlockHeader
	var r1 error
	if rf, ok := ret.Get(0).(func(*big.Int, *big.Int, *big.Int) ([]common.BlockHeader, error)); ok {
		return rf(chainId, from, to)
	}
	if rf, ok := ret.Get(0).(func(*big.Int, *big.Int, *big.Int) []common.BlockHeader); ok {
		r0 = rf(chainId, from, to)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]common.BlockHeader)
		}
	}

	if rf, ok := ret.Get(1).(func(*big.Int, *big.Int, *big.Int) error); ok {
		r1 = rf(chainId, from, to)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetBlockHeadersDescending_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetBlockHeadersDescending'
type MockIMainStorage_GetBlockHeadersDescending_Call struct {
	*mock.Call
}

// GetBlockHeadersDescending is a helper method to define mock.On call
//   - chainId *big.Int
//   - from *big.Int
//   - to *big.Int
func (_e *MockIMainStorage_Expecter) GetBlockHeadersDescending(chainId interface{}, from interface{}, to interface{}) *MockIMainStorage_GetBlockHeadersDescending_Call {
	return &MockIMainStorage_GetBlockHeadersDescending_Call{Call: _e.mock.On("GetBlockHeadersDescending", chainId, from, to)}
}

func (_c *MockIMainStorage_GetBlockHeadersDescending_Call) Run(run func(chainId *big.Int, from *big.Int, to *big.Int)) *MockIMainStorage_GetBlockHeadersDescending_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*big.Int), args[1].(*big.Int), args[2].(*big.Int))
	})
	return _c
}

func (_c *MockIMainStorage_GetBlockHeadersDescending_Call) Return(blockHeaders []common.BlockHeader, err error) *MockIMainStorage_GetBlockHeadersDescending_Call {
	_c.Call.Return(blockHeaders, err)
	return _c
}

func (_c *MockIMainStorage_GetBlockHeadersDescending_Call) RunAndReturn(run func(*big.Int, *big.Int, *big.Int) ([]common.BlockHeader, error)) *MockIMainStorage_GetBlockHeadersDescending_Call {
	_c.Call.Return(run)
	return _c
}

// GetBlocks provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetBlocks(qf storage.QueryFilter, fields ...string) (storage.QueryResult[common.Block], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetBlocks")
	}

	var r0 storage.QueryResult[common.Block]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) (storage.QueryResult[common.Block], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) storage.QueryResult[common.Block]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.Block])
	}

	if rf, ok := ret.Get(1).(func(storage.QueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetBlocks_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetBlocks'
type MockIMainStorage_GetBlocks_Call struct {
	*mock.Call
}

// GetBlocks is a helper method to define mock.On call
//   - qf storage.QueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetBlocks(qf interface{}, fields ...interface{}) *MockIMainStorage_GetBlocks_Call {
	return &MockIMainStorage_GetBlocks_Call{Call: _e.mock.On("GetBlocks",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetBlocks_Call) Run(run func(qf storage.QueryFilter, fields ...string)) *MockIMainStorage_GetBlocks_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.QueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetBlocks_Call) Return(blocks storage.QueryResult[common.Block], err error) *MockIMainStorage_GetBlocks_Call {
	_c.Call.Return(blocks, err)
	return _c
}

func (_c *MockIMainStorage_GetBlocks_Call) RunAndReturn(run func(storage.QueryFilter, ...string) (storage.QueryResult[common.Block], error)) *MockIMainStorage_GetBlocks_Call {
	_c.Call.Return(run)
	return _c
}

// GetLogs provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetLogs(qf storage.QueryFilter, fields ...string) (storage.QueryResult[common.Log], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetLogs")
	}

	var r0 storage.QueryResult[common.Log]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) (storage.QueryResult[common.Log], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) storage.QueryResult[common.Log]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.Log])
	}

	if rf, ok := ret.Get(1).(func(storage.QueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetLogs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLogs'
type MockIMainStorage_GetLogs_Call struct {
	*mock.Call
}

// GetLogs is a helper method to define mock.On call
//   - qf storage.QueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetLogs(qf interface{}, fields ...interface{}) *MockIMainStorage_GetLogs_Call {
	return &MockIMainStorage_GetLogs_Call{Call: _e.mock.On("GetLogs",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetLogs_Call) Run(run func(qf storage.QueryFilter, fields ...string)) *MockIMainStorage_GetLogs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.QueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetLogs_Call) Return(logs storage.QueryResult[common.Log], err error) *MockIMainStorage_GetLogs_Call {
	_c.Call.Return(logs, err)
	return _c
}

func (_c *MockIMainStorage_GetLogs_Call) RunAndReturn(run func(storage.QueryFilter, ...string) (storage.QueryResult[common.Log], error)) *MockIMainStorage_GetLogs_Call {
	_c.Call.Return(run)
	return _c
}

// GetMaxBlockNumber provides a mock function with given fields: chainId
func (_m *MockIMainStorage) GetMaxBlockNumber(chainId *big.Int) (*big.Int, error) {
	ret := _m.Called(chainId)

	if len(ret) == 0 {
		panic("no return value specified for GetMaxBlockNumber")
	}

	var r0 *big.Int
	var r1 error
	if rf, ok := ret.Get(0).(func(*big.Int) (*big.Int, error)); ok {
		return rf(chainId)
	}
	if rf, ok := ret.Get(0).(func(*big.Int) *big.Int); ok {
		r0 = rf(chainId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*big.Int)
		}
	}

	if rf, ok := ret.Get(1).(func(*big.Int) error); ok {
		r1 = rf(chainId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetMaxBlockNumber_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetMaxBlockNumber'
type MockIMainStorage_GetMaxBlockNumber_Call struct {
	*mock.Call
}

// GetMaxBlockNumber is a helper method to define mock.On call
//   - chainId *big.Int
func (_e *MockIMainStorage_Expecter) GetMaxBlockNumber(chainId interface{}) *MockIMainStorage_GetMaxBlockNumber_Call {
	return &MockIMainStorage_GetMaxBlockNumber_Call{Call: _e.mock.On("GetMaxBlockNumber", chainId)}
}

func (_c *MockIMainStorage_GetMaxBlockNumber_Call) Run(run func(chainId *big.Int)) *MockIMainStorage_GetMaxBlockNumber_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*big.Int))
	})
	return _c
}

func (_c *MockIMainStorage_GetMaxBlockNumber_Call) Return(maxBlockNumber *big.Int, err error) *MockIMainStorage_GetMaxBlockNumber_Call {
	_c.Call.Return(maxBlockNumber, err)
	return _c
}

func (_c *MockIMainStorage_GetMaxBlockNumber_Call) RunAndReturn(run func(*big.Int) (*big.Int, error)) *MockIMainStorage_GetMaxBlockNumber_Call {
	_c.Call.Return(run)
	return _c
}

// GetTokenBalances provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetTokenBalances(qf storage.BalancesQueryFilter, fields ...string) (storage.QueryResult[common.TokenBalance], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetTokenBalances")
	}

	var r0 storage.QueryResult[common.TokenBalance]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.BalancesQueryFilter, ...string) (storage.QueryResult[common.TokenBalance], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.BalancesQueryFilter, ...string) storage.QueryResult[common.TokenBalance]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.TokenBalance])
	}

	if rf, ok := ret.Get(1).(func(storage.BalancesQueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetTokenBalances_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTokenBalances'
type MockIMainStorage_GetTokenBalances_Call struct {
	*mock.Call
}

// GetTokenBalances is a helper method to define mock.On call
//   - qf storage.BalancesQueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetTokenBalances(qf interface{}, fields ...interface{}) *MockIMainStorage_GetTokenBalances_Call {
	return &MockIMainStorage_GetTokenBalances_Call{Call: _e.mock.On("GetTokenBalances",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetTokenBalances_Call) Run(run func(qf storage.BalancesQueryFilter, fields ...string)) *MockIMainStorage_GetTokenBalances_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.BalancesQueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetTokenBalances_Call) Return(_a0 storage.QueryResult[common.TokenBalance], _a1 error) *MockIMainStorage_GetTokenBalances_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockIMainStorage_GetTokenBalances_Call) RunAndReturn(run func(storage.BalancesQueryFilter, ...string) (storage.QueryResult[common.TokenBalance], error)) *MockIMainStorage_GetTokenBalances_Call {
	_c.Call.Return(run)
	return _c
}

// GetTokenTransfers provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetTokenTransfers(qf storage.TransfersQueryFilter, fields ...string) (storage.QueryResult[common.TokenTransfer], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetTokenTransfers")
	}

	var r0 storage.QueryResult[common.TokenTransfer]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.TransfersQueryFilter, ...string) (storage.QueryResult[common.TokenTransfer], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.TransfersQueryFilter, ...string) storage.QueryResult[common.TokenTransfer]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.TokenTransfer])
	}

	if rf, ok := ret.Get(1).(func(storage.TransfersQueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetTokenTransfers_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTokenTransfers'
type MockIMainStorage_GetTokenTransfers_Call struct {
	*mock.Call
}

// GetTokenTransfers is a helper method to define mock.On call
//   - qf storage.TransfersQueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetTokenTransfers(qf interface{}, fields ...interface{}) *MockIMainStorage_GetTokenTransfers_Call {
	return &MockIMainStorage_GetTokenTransfers_Call{Call: _e.mock.On("GetTokenTransfers",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetTokenTransfers_Call) Run(run func(qf storage.TransfersQueryFilter, fields ...string)) *MockIMainStorage_GetTokenTransfers_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.TransfersQueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetTokenTransfers_Call) Return(_a0 storage.QueryResult[common.TokenTransfer], _a1 error) *MockIMainStorage_GetTokenTransfers_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockIMainStorage_GetTokenTransfers_Call) RunAndReturn(run func(storage.TransfersQueryFilter, ...string) (storage.QueryResult[common.TokenTransfer], error)) *MockIMainStorage_GetTokenTransfers_Call {
	_c.Call.Return(run)
	return _c
}

// GetTraces provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetTraces(qf storage.QueryFilter, fields ...string) (storage.QueryResult[common.Trace], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetTraces")
	}

	var r0 storage.QueryResult[common.Trace]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) (storage.QueryResult[common.Trace], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) storage.QueryResult[common.Trace]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.Trace])
	}

	if rf, ok := ret.Get(1).(func(storage.QueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetTraces_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTraces'
type MockIMainStorage_GetTraces_Call struct {
	*mock.Call
}

// GetTraces is a helper method to define mock.On call
//   - qf storage.QueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetTraces(qf interface{}, fields ...interface{}) *MockIMainStorage_GetTraces_Call {
	return &MockIMainStorage_GetTraces_Call{Call: _e.mock.On("GetTraces",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetTraces_Call) Run(run func(qf storage.QueryFilter, fields ...string)) *MockIMainStorage_GetTraces_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.QueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetTraces_Call) Return(traces storage.QueryResult[common.Trace], err error) *MockIMainStorage_GetTraces_Call {
	_c.Call.Return(traces, err)
	return _c
}

func (_c *MockIMainStorage_GetTraces_Call) RunAndReturn(run func(storage.QueryFilter, ...string) (storage.QueryResult[common.Trace], error)) *MockIMainStorage_GetTraces_Call {
	_c.Call.Return(run)
	return _c
}

// GetTransactions provides a mock function with given fields: qf, fields
func (_m *MockIMainStorage) GetTransactions(qf storage.QueryFilter, fields ...string) (storage.QueryResult[common.Transaction], error) {
	_va := make([]interface{}, len(fields))
	for _i := range fields {
		_va[_i] = fields[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, qf)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetTransactions")
	}

	var r0 storage.QueryResult[common.Transaction]
	var r1 error
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) (storage.QueryResult[common.Transaction], error)); ok {
		return rf(qf, fields...)
	}
	if rf, ok := ret.Get(0).(func(storage.QueryFilter, ...string) storage.QueryResult[common.Transaction]); ok {
		r0 = rf(qf, fields...)
	} else {
		r0 = ret.Get(0).(storage.QueryResult[common.Transaction])
	}

	if rf, ok := ret.Get(1).(func(storage.QueryFilter, ...string) error); ok {
		r1 = rf(qf, fields...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_GetTransactions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTransactions'
type MockIMainStorage_GetTransactions_Call struct {
	*mock.Call
}

// GetTransactions is a helper method to define mock.On call
//   - qf storage.QueryFilter
//   - fields ...string
func (_e *MockIMainStorage_Expecter) GetTransactions(qf interface{}, fields ...interface{}) *MockIMainStorage_GetTransactions_Call {
	return &MockIMainStorage_GetTransactions_Call{Call: _e.mock.On("GetTransactions",
		append([]interface{}{qf}, fields...)...)}
}

func (_c *MockIMainStorage_GetTransactions_Call) Run(run func(qf storage.QueryFilter, fields ...string)) *MockIMainStorage_GetTransactions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(storage.QueryFilter), variadicArgs...)
	})
	return _c
}

func (_c *MockIMainStorage_GetTransactions_Call) Return(transactions storage.QueryResult[common.Transaction], err error) *MockIMainStorage_GetTransactions_Call {
	_c.Call.Return(transactions, err)
	return _c
}

func (_c *MockIMainStorage_GetTransactions_Call) RunAndReturn(run func(storage.QueryFilter, ...string) (storage.QueryResult[common.Transaction], error)) *MockIMainStorage_GetTransactions_Call {
	_c.Call.Return(run)
	return _c
}

// InsertBlockData provides a mock function with given fields: data
func (_m *MockIMainStorage) InsertBlockData(data []common.BlockData) error {
	ret := _m.Called(data)

	if len(ret) == 0 {
		panic("no return value specified for InsertBlockData")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func([]common.BlockData) error); ok {
		r0 = rf(data)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockIMainStorage_InsertBlockData_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InsertBlockData'
type MockIMainStorage_InsertBlockData_Call struct {
	*mock.Call
}

// InsertBlockData is a helper method to define mock.On call
//   - data []common.BlockData
func (_e *MockIMainStorage_Expecter) InsertBlockData(data interface{}) *MockIMainStorage_InsertBlockData_Call {
	return &MockIMainStorage_InsertBlockData_Call{Call: _e.mock.On("InsertBlockData", data)}
}

func (_c *MockIMainStorage_InsertBlockData_Call) Run(run func(data []common.BlockData)) *MockIMainStorage_InsertBlockData_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]common.BlockData))
	})
	return _c
}

func (_c *MockIMainStorage_InsertBlockData_Call) Return(_a0 error) *MockIMainStorage_InsertBlockData_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockIMainStorage_InsertBlockData_Call) RunAndReturn(run func([]common.BlockData) error) *MockIMainStorage_InsertBlockData_Call {
	_c.Call.Return(run)
	return _c
}

// ReplaceBlockData provides a mock function with given fields: data
func (_m *MockIMainStorage) ReplaceBlockData(data []common.BlockData) ([]common.BlockData, error) {
	ret := _m.Called(data)

	if len(ret) == 0 {
		panic("no return value specified for ReplaceBlockData")
	}

	var r0 []common.BlockData
	var r1 error
	if rf, ok := ret.Get(0).(func([]common.BlockData) ([]common.BlockData, error)); ok {
		return rf(data)
	}
	if rf, ok := ret.Get(0).(func([]common.BlockData) []common.BlockData); ok {
		r0 = rf(data)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]common.BlockData)
		}
	}

	if rf, ok := ret.Get(1).(func([]common.BlockData) error); ok {
		r1 = rf(data)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockIMainStorage_ReplaceBlockData_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReplaceBlockData'
type MockIMainStorage_ReplaceBlockData_Call struct {
	*mock.Call
}

// ReplaceBlockData is a helper method to define mock.On call
//   - data []common.BlockData
func (_e *MockIMainStorage_Expecter) ReplaceBlockData(data interface{}) *MockIMainStorage_ReplaceBlockData_Call {
	return &MockIMainStorage_ReplaceBlockData_Call{Call: _e.mock.On("ReplaceBlockData", data)}
}

func (_c *MockIMainStorage_ReplaceBlockData_Call) Run(run func(data []common.BlockData)) *MockIMainStorage_ReplaceBlockData_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]common.BlockData))
	})
	return _c
}

func (_c *MockIMainStorage_ReplaceBlockData_Call) Return(_a0 []common.BlockData, _a1 error) *MockIMainStorage_ReplaceBlockData_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockIMainStorage_ReplaceBlockData_Call) RunAndReturn(run func([]common.BlockData) ([]common.BlockData, error)) *MockIMainStorage_ReplaceBlockData_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockIMainStorage creates a new instance of MockIMainStorage. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockIMainStorage(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockIMainStorage {
	mock := &MockIMainStorage{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

import Vue from 'vue'
import Vuex from 'vuex'

import { fetchTwitterRequests, fetchStockRequests,
  fetchTwitterWeekly, fetchStockWeekly,
  makeTwitterRequest, makeStockRequest } from '@/api'
import { EventBus } from '@/utils'

Vue.use(Vuex)

const state = {
  twitterRequests: [],
  stockRequests: [],
  twitterWeekly: [],
  stockWeekly: []
}

const actions = {
  loadTwitterRequests (context) {
    return fetchTwitterRequests().then(response =>
      context.commit('setTwitterRequests', {
        twitterRequests: response.data
      })
    )
  },
  requestNewTwitterUser (context, data) {
    return makeTwitterRequest(data.twitterUser).then(response => {
      EventBus.$emit('twitterRequestSucceeded', response.data.message)
      context.dispatch('loadTwitterRequests')
    }).catch(error => {
      EventBus.$emit('twitterRequestFailed', error.response.data.message)
    })
  },
  loadTwitterWeekly (context) {
    return fetchTwitterWeekly().then(response => {
      console.log(response.data)
      context.commit('setTwitterWeekly', {
        twitterWeekly: response.data
      })
    })
  },
  loadStockRequests (context) {
    return fetchStockRequests().then(response =>
      context.commit('setStockRequests', {
        stockRequests: response.data
      })
    )
  },
  requestNewStockTicker (context, data) {
    return makeStockRequest(data.stockTicker).then(response => {
      EventBus.$emit('stockRequestSucceeded', response.data.message)
      context.dispatch('loadStockRequests')
    }).catch(error => {
      EventBus.$emit('stockRequestFailed', error.response.data.message)
    })
  },
  loadStockWeekly (context) {
    return fetchStockWeekly().then(response => {
      console.log(response.data)
      context.commit('setStockWeekly', {
        stockWeekly: response.data
      })
    })
  }
}

const mutations = {
  setTwitterRequests (state, payload) {
    state.twitterRequests = payload.twitterRequests
  },
  setStockRequests (state, payload) {
    state.stockRequests = payload.stockRequests
  },
  setTwitterWeekly (state, payload) {
    state.twitterWeekly = payload.twitterWeekly
  },
  setStockWeekly (state, payload) {
    state.stockWeekly = payload.stockWeekly
  }
}

const getters = {}

const store = new Vuex.Store({
  state,
  actions,
  mutations,
  getters
})

export default store

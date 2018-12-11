import Vue from 'vue'
import Vuex from 'vuex'

import { fetchTwitterRequests } from '@/api'

Vue.use(Vuex)

const state = {
  twitterRequests: [],
  stockRequests: []
}

const actions = {
  loadTwitterRequests (context) {
    return fetchTwitterRequests().then(response =>
      context.commit('setTwitterRequests', { twitterRequests: response.data })
    )
  },
  requestNewTwitterUser (context, data) {
    // TODO
  }
}

const mutations = {
  setTwitterRequests (state, payload) {
    state.twitterRequests = payload.twitterRequests
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

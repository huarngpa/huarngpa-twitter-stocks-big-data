import Vue from 'vue'
import Vuex from 'vuex'

Vue.use(Vuex)

const state = {
  requests: []
}

const actions = {
  loadRequests (context) {
  }
}

const mutations = {
  setRequests (state, payload) {
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
